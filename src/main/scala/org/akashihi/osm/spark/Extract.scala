package org.akashihi.osm.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.jts.geom._

object Extract {

  sealed trait ExtractPolicy

  case object Simple extends ExtractPolicy

  case object CompleteWays extends ExtractPolicy

  case object CompleteRelations extends ExtractPolicy

  case object ReferenceComplete extends ExtractPolicy

  case object ParentRelations extends ExtractPolicy

  private def extract(spark: SparkSession, relation_content: DataFrame, boundary: Polygon): (DataFrame, DataFrame, DataFrame) = {
    val envelope = boundary.getEnvelopeInternal
    //The extract step - mark all nodes that fit the bounding box
    val nodesRect = relation_content.filter(col("TYPE") === OsmEntity.NODE && col("LON") >= envelope.getMinX && col("LON") <= envelope.getMaxX && col("LAT") >= envelope.getMinY && col("LAT") <= envelope.getMaxY).cache()

    val nodes = if (boundary.isRectangle) {
      nodesRect
    } else {
      val geometryFactory = new GeometryFactory()
      val polygonFilterUdf = udf { (lon: Double, lat: Double) => boundary.contains(geometryFactory.createPoint(new Coordinate(lon, lat)))}
      nodesRect.filter(polygonFilterUdf(col("LON"), col("LAT")))
    }

    //Mandatory way step - get all ways, matching selected nodes
    val nodes_ids = nodes.select("ID").collect().map(_.getAs[Long]("ID")).toSet
    val nodes_ids_bc = spark.sparkContext.broadcast(nodes_ids)
    val ways = relation_content.filter(col("TYPE") === OsmEntity.WAY).filter(row => Option(row.getAs[Seq[Long]]("WAY")).exists(_.exists(nodes_ids_bc.value.contains(_))))

    //Mandatory relation step - get all relations, mentioning selected ways or nodes
    val ways_ids = ways.select("ID").collect().map(_.getAs[Long]("ID")).toSet
    val ways_ids_bc = spark.sparkContext.broadcast(ways_ids)

    val relations = relation_content.filter(col("TYPE") === OsmEntity.RELATION).filter(row => {
      val node_match = Option(row.getAs[Seq[Long]]("RELATION_NODES")).exists(_.exists(nodes_ids_bc.value.contains(_)))
      val way_match = Option(row.getAs[Seq[Long]]("RELATION_WAYS")).exists(_.exists(ways_ids_bc.value.contains(_)))
      node_match || way_match
    })
    (nodes, ways, relations)
  }

  private def filterRelationsByIndex(relations: DataFrame, osm: DataFrame, index: Map[Long, Seq[Long]], spark: SparkSession): DataFrame = {
    val index_bc = spark.sparkContext.broadcast(index)

    def indexMapper(id: Long): Option[Seq[Long]] = index_bc.value.get(id)

    val indexMapperUdf = udf(indexMapper _)

    val selectedIds = relations.select("ID")
      .withColumn("PARENTS", indexMapperUdf(col("ID")))
      .withColumn("PARENT", explode(col("PARENTS")))
      .select("PARENT").collect()
      .map(_.getAs[Long]("PARENT"))
      .toSet
    val selectedIds_bc = spark.sparkContext.broadcast(selectedIds)

    osm.filter(col("TYPE") === OsmEntity.RELATION).filter(row => selectedIds_bc.value.contains(row.getAs[Long]("ID")))
  }

  private def extractReferencedRelations(relations: DataFrame, osm: DataFrame, policy: ExtractPolicy, spark: SparkSession): DataFrame = {
    val relationsMembers = Relation.getRelationsDescendants(osm)
    val parents = if (policy == ParentRelations) {
      val parentIndex = Relation.makeRelationParentIndex(osm, relationsMembers).map(identity)
      filterRelationsByIndex(relations, osm, parentIndex, spark).union(relations)
    } else {
      relations
    }
    val childrenIndex = Relation.makeRelationChildrenIndex(osm, relationsMembers).map(identity)
    val children = filterRelationsByIndex(parents, osm, childrenIndex, spark)

    parents.union(children).dropDuplicates("ID", "TYPE")
  }

  /**
   * Extracts an area from the osm dataset.
   * @param osm OSM dataframe to work on.
   * @param left Leftmost coordinate of the bbox (min lon)
   * @param top Topmost coordinate of the bbox (max lat)
   * @param right Rightmost coordinate of the bbox (max lon)
   * @param bottom Lowest coordinate of the bbox (min lat)
   * @param policy Defines how to to the extract. There are five policies defined:
   *               - ExtractPolicy.Simple (default) - Will extract only nodes inside of the bbox, ways, referenced by
   *                 those nodes and relations, referenced by those nodes and ways. Ways that cross edge of the bbox
   *                 may be left incomplete, relations may be left incomplete.
   *               - ExtractPolicy.CompleteWays - same as simple plus all nodes, referenced by ways, will be added
   *                 to the extract, making ways complete. Relations still may be left incomplete.
   *               - ExtractPolicy.CompleteRelations - same as CompleteWays plus all nodes and ways,
   *                 referenced by included relations, will be added to the extract, making relations almost complete.
   *                 All referenced ways will also be complete. Children relations of included relations still be missing.
   *               - ExtractPolicy.ReferenceComplete - same as CompleteRelations, but also full children hierarchy of
   *                 included relations will be added to extract, including all the ways and nodes, referenced by
   *                 children relations. All ways will be complete.
   *               - ExtractPolicy.ParentRelations - same as ReferenceComplete and also adds all parent relations of
   *                 included relations and all children of those parents, with their ways and nodes. All ways and
   *                 relations will be reference complete.
   *
   * Simple will produce smallest and most precise extract, while other policies may add more and more data
   * outside of the bbox area. ReferenceComplete policy is usually enough for any practical use. ParentRelations
   * may (and will) include a lot of data outside of the bounding box.
   * @param spark spark session to use.
   * @return OSM dataframe cropped to the specified bounding box using specified policy.
   */
  def apply(osm: DataFrame, left: Double, top: Double, right: Double, bottom: Double, policy: ExtractPolicy, spark: SparkSession): DataFrame = {
    val points = Seq(
      Seq(left, top),
      Seq(right, top),
      Seq(right, bottom),
      Seq(left, bottom)
    )
    apply(osm, points, policy, spark)
  }

  /**
   * Extracts an area from the osm dataset.
   * @param osm OSM dataframe to work on.
   * @param coordinates Extraction polygin defined as a sequence of points
   * @param policy Defines how to to the extract. There are five policies defined:
   *               - ExtractPolicy.Simple (default) - Will extract only nodes inside of the bbox, ways, referenced by
   *                 those nodes and relations, referenced by those nodes and ways. Ways that cross edge of the bbox
   *                 may be left incomplete, relations may be left incomplete.
   *               - ExtractPolicy.CompleteWays - same as simple plus all nodes, referenced by ways, will be added
   *                 to the extract, making ways complete. Relations still may be left incomplete.
   *               - ExtractPolicy.CompleteRelations - same as CompleteWays plus all nodes and ways,
   *                 referenced by included relations, will be added to the extract, making relations almost complete.
   *                 All referenced ways will also be complete. Children relations of included relations still be missing.
   *               - ExtractPolicy.ReferenceComplete - same as CompleteRelations, but also full children hierarchy of
   *                 included relations will be added to extract, including all the ways and nodes, referenced by
   *                 children relations. All ways will be complete.
   *               - ExtractPolicy.ParentRelations - same as ReferenceComplete and also adds all parent relations of
   *                 included relations and all children of those parents, with their ways and nodes. All ways and
   *                 relations will be reference complete.
   *
   * Simple will produce smallest and most precise extract, while other policies may add more and more data
   * outside of the bbox area. ReferenceComplete policy is usually enough for any practical use. ParentRelations
   * may (and will) include a lot of data outside of the bounding box.
   * @param spark spark session to use.
   * @return OSM dataframe cropped to the specified bounding box using specified policy.
   */
  def apply(osm: DataFrame, coordinates: Seq[Seq[Double]], policy: ExtractPolicy, spark: SparkSession): DataFrame = {
    val geometryFactory = new GeometryFactory()
    val polygon = geometryFactory.createPolygon(coordinates.map(point => new Coordinate(point.head, point.last)).toArray)
    apply(osm, polygon, policy, spark)
  }

  /**
   * Extracts an area from the osm dataset.
   * @param osm OSM dataframe to work on.
   * @param boundary Extraction polygon object.
   * @param policy Defines how to to the extract. There are five policies defined:
   *               - ExtractPolicy.Simple (default) - Will extract only nodes inside of the bbox, ways, referenced by
   *                 those nodes and relations, referenced by those nodes and ways. Ways that cross edge of the bbox
   *                 may be left incomplete, relations may be left incomplete.
   *               - ExtractPolicy.CompleteWays - same as simple plus all nodes, referenced by ways, will be added
   *                 to the extract, making ways complete. Relations still may be left incomplete.
   *               - ExtractPolicy.CompleteRelations - same as CompleteWays plus all nodes and ways,
   *                 referenced by included relations, will be added to the extract, making relations almost complete.
   *                 All referenced ways will also be complete. Children relations of included relations still be missing.
   *               - ExtractPolicy.ReferenceComplete - same as CompleteRelations, but also full children hierarchy of
   *                 included relations will be added to extract, including all the ways and nodes, referenced by
   *                 children relations. All ways will be complete.
   *               - ExtractPolicy.ParentRelations - same as ReferenceComplete and also adds all parent relations of
   *                 included relations and all children of those parents, with their ways and nodes. All ways and
   *                 relations will be reference complete.
   *
   * Simple will produce smallest and most precise extract, while other policies may add more and more data
   * outside of the bbox area. ReferenceComplete policy is usually enough for any practical use. ParentRelations
   * may (and will) include a lot of data outside of the bounding box.
   * @param spark spark session to use.
   * @return OSM dataframe cropped to the specified bounding box using specified policy.
   */
  def apply(osm: DataFrame, boundary: Polygon, policy: ExtractPolicy, spark: SparkSession): DataFrame = {
    val relation_content = Relation.makeRelationsContent(osm)

    val (extracted_nodes, extracted_ways, extracted_relations) = extract(spark, relation_content, boundary)

    val referencedRelations = if (policy == ReferenceComplete || policy == ParentRelations) {
      extractReferencedRelations(extracted_relations, relation_content, policy, spark).union(extracted_relations).dropDuplicates("ID", "TYPE")
    } else {
      extracted_relations
    }

    val (completeNodes, completeWays) = if (policy == CompleteRelations || policy == ReferenceComplete || policy == ParentRelations) {
      val referencedNodes = referencedRelations.filter(col("TYPE") === OsmEntity.RELATION)
        .filter(size(col("RELATION_NODES")) > 0)
        .select("RELATION_NODES")
        .withColumn("NODES", explode(col("RELATION_NODES")))
        .select("NODES").distinct()
        .collect().map(_.getAs[Long]("NODES")).toSet

      val referencedWays = referencedRelations.filter(col("TYPE") === OsmEntity.RELATION)
        .filter(size(col("RELATION_WAYS")) > 0)
        .select("RELATION_WAYS")
        .withColumn("WAYS", explode(col("RELATION_WAYS")))
        .select("WAYS").distinct()
        .collect().map(_.getAs[Long]("WAYS")).toSet

      val referencedNodes_bc = spark.sparkContext.broadcast(referencedNodes)
      val referencedWays_bc = spark.sparkContext.broadcast(referencedWays)
      val selectedNodes = relation_content.filter(col("TYPE") === OsmEntity.NODE).filter(row => referencedNodes_bc.value.contains(row.getAs[Long]("ID"))).union(extracted_nodes).dropDuplicates("ID", "TYPE")
      val selectedWays = relation_content.filter(col("TYPE") === OsmEntity.WAY).filter(row => referencedWays_bc.value.contains(row.getAs[Long]("ID"))).union(extracted_ways).dropDuplicates("ID", "TYPE")
      (selectedNodes, selectedWays)
    } else {
      (extracted_nodes, extracted_ways)
    }

    val allWaysNodes = if (policy != Simple) {
      val allNodes = completeWays.filter(col("TYPE") === OsmEntity.WAY)
        .select("WAY")
        .withColumn("NODES", explode(col("WAY")))
        .select("NODES")
          .distinct()
          .collect()
          .map(_.getAs[Long]("NODES"))
          .toSet
      val allNodes_bc = spark.sparkContext.broadcast(allNodes)
      relation_content.filter(col("TYPE") === OsmEntity.NODE).filter(row => allNodes_bc.value.contains(row.getAs[Long]("ID"))).union(completeNodes).dropDuplicates("ID", "TYPE")
    } else {
      completeNodes
    }

    allWaysNodes.union(completeWays).union(referencedRelations)
  }
}
