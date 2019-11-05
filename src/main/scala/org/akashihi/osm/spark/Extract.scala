package org.akashihi.osm.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object Extract {
  private def valueInArray(array: mutable.WrappedArray[Long], value: Long): Boolean = array != null && array.contains(value)
  private def valueInArrayUdf = udf(valueInArray _)

  private def relationMemberIdByType(relation: mutable.WrappedArray[Row], entity_type: Int): Option[mutable.WrappedArray[Long]] = {
    Option(relation).map(_.filter(_.getAs[Int]("TYPE") == entity_type).map(_.getAs[Long]("ID")))
  }
  private def relationMemberIdByTypeUdf = udf(relationMemberIdByType _)

  private def getRelationMembersLists(osm: DataFrame, entity_type: Int): Map[Long, List[Long]] = osm.filter(col("TYPE") === OsmEntity.RELATION)
    .select("ID", "RELATION")
    .withColumn("REL_MEMBERS", relationMemberIdByTypeUdf(col("RELATION"), lit(entity_type)))
    .filter(size(col("REL_MEMBERS")) > 0)
    .collect()
    .par.map(row => (row.getAs[Long]("ID"), row.getAs[Seq[Long]]("REL_MEMBERS")))
    .flatMap(row => row._2.map(member => (member, row._1))) // Here and in following 3 line we invert keys and value
    .groupBy(_._1)                                          // So member id becomes keys and relation id's are values
    .mapValues(_.map(_._2).toList).seq.toMap


  private def matchRelationByMember(nodes: Map[Long, Seq[Long]], ways: Map[Long, Seq[Long]])(id: Long, entity_type: Int): Seq[Long] = entity_type match {
      case OsmEntity.NODE => nodes.getOrElse(id, Seq[Long]())
      case OsmEntity.WAY => ways.getOrElse(id, Seq[Long]())
    }

  def apply(spark: SparkSession, osm: DataFrame, left: Double, top: Double, right: Double, bottom: Double): DataFrame = {

    //The extract step - mark all nodes that fit the bounding box
    val extracted = osm.withColumn("USE_NODES", when(col("TYPE") === OsmEntity.NODE && col("LON") >= left && col("LON") <= right && col("LAT") >= bottom && col("LAT") <= top, lit(true)).otherwise(lit(false)))

    //Mandatory way step - get all ways, matching selected nodes
    val nodes = extracted.filter("USE_NODES").select("ID").withColumnRenamed("ID", "NODE_ID")
    val extracted_ways = extracted.join(nodes, valueInArrayUdf(col("WAY"), col("NODE_ID")), "leftouter")
      .withColumn("USE_WAYS", col("NODE_ID").isNotNull && col("TYPE") === OsmEntity.WAY).persist(StorageLevel.MEMORY_AND_DISK)

    //Mandatory relation step - get all relations, mentioning selected ways or nodes
    val relation_nodes =getRelationMembersLists(osm, OsmEntity.NODE).map(identity)
    val relation_ways =getRelationMembersLists(osm, OsmEntity.WAY).map(identity)

    val relation_nodes_bc = spark.sparkContext.broadcast(relation_nodes)
    val relation_ways_bc = spark.sparkContext.broadcast(relation_ways)
    val relationMatcher = matchRelationByMember(relation_nodes_bc.value, relation_ways_bc.value) _
    val relationMatcherUdf = udf(relationMatcher)
    val selectedRelations = extracted_ways.filter(col("TYPE") =!= OsmEntity.RELATION)
      .filter(col("USE_WAYS") || col("USE_NODES"))
      .select("ID", "TYPE")
      .withColumn("SELECTED_RELATIONS", relationMatcherUdf(col("ID"), col("TYPE")))
      .select("SELECTED_RELATIONS")
      .collect().par.flatMap(_.getAs[Seq[Long]]("SELECTED_RELATIONS")).distinct.seq.toList

    val selectedRelationsBc = spark.sparkContext.broadcast(selectedRelations)
    val extracted_relations = extracted_ways.withColumn("USE_RELATIONS", col("TYPE") === OsmEntity.RELATION && col("ID").isInCollection(selectedRelationsBc.value))
        .drop("NODE_ID")

    // Handle "Referentially complete"

    // Handle "Relation complete"

    // Handle "Way complete"

    extracted_relations.filter(col("USE_NODES") || col("USE_WAYS") || col("USE_RELATIONS"))
      .drop("USE_NODES", "USE_WAYS", "USE_RELATIONS")
  }
}
