package org.akashihi.osm.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import scala.annotation.tailrec
import scala.collection.mutable

object Extract {

  sealed trait ExtractPolicy

  case object Simple extends ExtractPolicy

  case object CompleteWays extends ExtractPolicy

  case object CompleteRelations extends ExtractPolicy

  case object ReferenceComplete extends ExtractPolicy

  case object ParentRelations extends ExtractPolicy

  private def extract(spark: SparkSession, relation_content: DataFrame, left: Double, top: Double, right: Double, bottom: Double): (DataFrame, DataFrame, DataFrame) = {
    //The extract step - mark all nodes that fit the bounding box
    val nodes = relation_content.filter(col("TYPE") === OsmEntity.NODE && col("LON") >= left && col("LON") <= right && col("LAT") >= bottom && col("LAT") <= top).cache()

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
    val index_bx = spark.sparkContext.broadcast(index)

    def indexMapper(id: Long): Option[Seq[Long]] = index_bx.value.get(id)

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
    val children = filterRelationsByIndex(relations, osm, childrenIndex, spark)

    parents.union(children).dropDuplicates("ID", "TYPE")
  }

  def apply(osm: DataFrame, left: Double, top: Double, right: Double, bottom: Double, policy: ExtractPolicy = Simple, spark: SparkSession): DataFrame = {
    val relation_content = Relation.makeRelationsContent(osm)

    val (extracted_nodes, extracted_ways, extracted_relations) = extract(spark, relation_content, left, top, right, bottom)

    val referencedRelations = if (policy == ReferenceComplete || policy == ParentRelations) {
      //Do handling
      extractReferencedRelations(extracted_relations, relation_content, policy, spark).union(extracted_relations)
    } else {
      extracted_relations
    }

    /*val referencedRelations = if (policy == CompleteRelations || policy == ReferenceComplete || policy == ParentRelations) {
      relationsHierarchy
    } else {
      relationsHierarchy
    }

    val completeWays = if (policy != Simple) {
      //Do handlind
      referencedRelations
    } else {
      referencedRelations
    }

    completeWays.filter(col("USE_NODES") || col("USE_WAYS") || col("USE_RELATIONS"))
      .drop("USE_NODES", "USE_WAYS", "USE_RELATIONS")*/
    extracted_nodes.union(extracted_ways).union(referencedRelations)
  }
}
