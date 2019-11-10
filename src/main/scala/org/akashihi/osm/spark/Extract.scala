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

  private def valueInArray(array: mutable.WrappedArray[Long], value: Long): Boolean = array != null && array.contains(value)
  private def valueInArrayUdf = udf(valueInArray _)

  /**
   * Extracts relation member IDs with the specified from the list of all the relation members.
   *
   * @param relation    List of all the relations members.
   * @param entity_type OSM entity type of member to extract.
   * @return List of relation members IDs with the specified OSM entity type.
   */
  private def relationMemberIdByType(relation: mutable.WrappedArray[Row], entity_type: Int): Option[mutable.WrappedArray[Long]] = {
    Option(relation).map(_.filter(_.getAs[Int]("TYPE") == entity_type).map(_.getAs[Long]("ID")))
  }
  private def relationMemberIdByTypeUdf = udf(relationMemberIdByType _)

  /**
   * Extracts relation members ID, filtering them by type.
   *
   * @param osm         OSM dataframe with relations to process.
   * @param entity_type OSM entity type to filter.
   * @return Map with relation IDs as keys and their members lists, filtered by OSM entity type, as values.
   */
  private def getRelationMembersList(osm: DataFrame, entity_type: Int): Map[Long, Seq[Long]] = osm.filter(col("TYPE") === OsmEntity.RELATION)
    .select("ID", "RELATION")
    .withColumn("REL_MEMBERS", relationMemberIdByTypeUdf(col("RELATION"), lit(entity_type)))
    .filter(size(col("REL_MEMBERS")) > 0)
    .collect()
    .par.map(row => (row.getAs[Long]("ID"), row.getAs[Seq[Long]]("REL_MEMBERS"))).seq.toMap

  /**
   * Build a map of all the relations, mentioning member of a type.
   *
   * @param osm         OSM dataframe with relations to process
   * @param entity_type OSM entity type to filter.
   * @return Map with entity IDs as keys and list of relations, mentioning them, as values. So it is just inverted getRelationMembersList
   */
  private def mapMembersToRelations(osm: DataFrame, entity_type: Int): Map[Long, List[Long]] = getRelationMembersList(osm, entity_type).par
    .flatMap { case (rel, members) => members.map(member => (member, rel)) }
    .groupBy(_._1)
    .mapValues(_.values.seq.toList).seq.toMap


  private def matchRelationByMember(nodes: Map[Long, Seq[Long]], ways: Map[Long, Seq[Long]])(id: Long, entity_type: Int): Seq[Long] = entity_type match {
    case OsmEntity.NODE => nodes.getOrElse(id, Seq[Long]())
    case OsmEntity.WAY => ways.getOrElse(id, Seq[Long]())
  }

  def extract(spark: SparkSession, osm: DataFrame, left: Double, top: Double, right: Double, bottom: Double): DataFrame = {
    val relation_content = Relation.makeRelationsContent(osm)

    //The extract step - mark all nodes that fit the bounding box
    val nodes = relation_content.filter(col("TYPE") === OsmEntity.NODE && col("LON") >= left && col("LON") <= right && col("LAT") >= bottom && col("LAT") <= top).cache()

    //Mandatory way step - get all ways, matching selected nodes
    val nodes_ids = nodes.select("ID").collect().map(_.getAs[Long]("ID")).toSet
    val nodes_ids_bc = spark.sparkContext.broadcast(nodes_ids)
    val ways = relation_content.filter(col("TYPE") === OsmEntity.NODE).filter(row => Option(row.getAs[Seq[Long]]("WAY")).exists(_.exists(nodes_ids_bc.value.contains(_))))

    //Mandatory relation step - get all relations, mentioning selected ways or nodes
    val ways_ids = ways.select("ID").collect().map(_.getAs[Long]("ID")).toSet
    val ways_ids_bc = spark.sparkContext.broadcast(ways_ids)

    val relations = relation_content.filter(col("TYPE") === OsmEntity.RELATION).filter(row => {
      val node_match = Option(row.getAs[Seq[Long]]("RELATION_NODES")).exists(_.exists(nodes_ids_bc.value.contains(_)))
      val way_match = Option(row.getAs[Seq[Long]]("RELATION_WAYS")).exists(_.exists(ways_ids_bc.value.contains(_)))
      node_match || way_match
    })
    nodes.union(ways).union(relations)
  }

  private def relationChildList(relations: Map[Long, Seq[Long]])(id: Long):Seq[Long] = {
    @tailrec
    def iterateRelation(relation: Seq[Long], siblings: Seq[Long]):Seq[Long] = {
      relations.get(id) match {
        case None => siblings
        case Some(sibling) => iterateRelation(sibling.tail, siblings :+ sibling.head)
      }
    }

    relations.get(id).map(iterateRelation(_, Seq[Long](id))).getOrElse(Seq[Long]())
  }

  private def markDescendantRelations(spark: SparkSession, osm: DataFrame, policy: ExtractPolicy): DataFrame = {
    val relation_relations = getRelationMembersList(osm, OsmEntity.RELATION).map(identity).filter { case (_, v) => v.nonEmpty }
    val relation_relations_bc = spark.sparkContext.broadcast(relation_relations)

    val relationChildListBc = relationChildList(relation_relations_bc.value) _
    val relationChildListUdf = udf(relationChildListBc)

    val relations_with_kids = osm.filter(col("TYPE") === OsmEntity.RELATION)
      .withColumn("RELATION_KIDS", relationChildListUdf(col("ID")))
    relations_with_kids
      .filter(size(col("RELATION_KIDS"))>0)
      .show(false)
    throw new RuntimeException("stop")
    relations_with_kids
  }

  def apply(osm: DataFrame, left: Double, top: Double, right: Double, bottom: Double, policy: ExtractPolicy = Simple, spark: SparkSession): DataFrame = {
    val extracted_relations = extract(spark, osm, left, top, right, bottom)

    /*val relationsHierarchy = if (policy == ReferenceComplete || policy == ParentRelations) {
      //Do handling
      //markDescendantRelations(spark, relationsHierarchy, policy)
      extracted_relations
    } else {
      extracted_relations
    }

    val referencedRelations = if (policy == CompleteRelations || policy == ReferenceComplete || policy == ParentRelations) {
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
    extracted_relations
  }
}
