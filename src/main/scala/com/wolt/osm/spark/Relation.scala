package com.wolt.osm.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.parallel.{ParIterable, ParSeq}

object Relation {
  private val log = Logger.getLogger(getClass.getName)

  /**
   * Extracts relation member IDs with the specified from the list of all the relation members.
   *
   * @param relation    List of all the relations members.
   * @param entity_type OSM entity type of member to extract.
   * @return List of relation members IDs with the specified OSM entity type.
   */
  private def relationMemberIdByType(relation: mutable.WrappedArray[Row], entity_type: Int): Option[mutable.WrappedArray[Long]] = {
    Option(relation).map(_.filter(_.getAs[Int]("TYPE") == entity_type)
      .map(_.getAs[Long]("ID")))
      .flatMap(v => if (v.nonEmpty) {
        Some(v)
      } else {
        None
      })
  }

  private def relationMemberIdByTypeUdf = udf(relationMemberIdByType _)

  private def findNextParent(id: Long, invertedMembers: Map[Long, ParIterable[Long]], seen: Set[Long] = Set[Long]()): ParIterable[Long] = {
    if (seen.contains(id)) {
      log.warn(s"Found cyclic relation with id $id")
      ParSeq()
    } else {
      if (invertedMembers.contains(id)) {
        invertedMembers(id).flatMap(p => findNextParent(p, invertedMembers, seen + id))
      } else {
        ParSeq(id)
      }
    }
  }

  private def expandKids(kid: Long, members: Map[Long, Seq[Long]], seen: Set[Long] = Set[Long]()): Seq[Long] = {
    if (seen.contains(kid)) {
      log.warn(s"Found cyclic relation with id $kid")
      Seq()
    } else {
      if (members.contains(kid)) {
        Seq(kid) ++ members(kid).flatMap(k => expandKids(k, members, seen + kid))
      } else {
        Seq(kid)
      }
    }
  }

  private def ensureRelationContentPresent(osm: DataFrame): DataFrame = if (osm.schema.fields.map(_.name).contains("RELATION_RELATIONS")) {
    osm
  } else {
    makeRelationsContent(osm)
  }

  /**
   * Adds relations member information to the dataframe.
   * Will add 3 new columns: RELATION_NODES, RELATION_WAYS, RELATION_RELATIONS
   * and fill them, for each relation, with the IDs of the relation members of the
   * corresponding type, preserving the order. For non relation entries or of relations without
   * some specific member type, values of those columns will be null.
   *
   * @param osm OSM dataframe.
   * @return Same dataframe with RELATION_NODES, RELATION_WAYS and RELATION_RELATIONS columns
   */
  def makeRelationsContent(osm: DataFrame): DataFrame = {
    osm.withColumn("RELATION_NODES", relationMemberIdByTypeUdf(col("RELATION"), lit(OsmEntity.NODE)))
      .withColumn("RELATION_WAYS", relationMemberIdByTypeUdf(col("RELATION"), lit(OsmEntity.WAY)))
      .withColumn("RELATION_RELATIONS", relationMemberIdByTypeUdf(col("RELATION"), lit(OsmEntity.RELATION)))
  }

  /**
   * Extracts relation hierarchy data from the OSM data frame. Will return map of relation
   * IDs and their members of 'RELATION' type, skipping relations, having no subrelations.
   *
   * @param osm OSM dataframe.
   * @return Map of relation IDs and list of their direct descendants IDs.
   */
  def getRelationsDescendants(osm: DataFrame): Map[Long, Seq[Long]] = {
    val withContent = ensureRelationContentPresent(osm)

    withContent.filter(col("TYPE") === OsmEntity.RELATION)
      .filter(size(col("RELATION_RELATIONS")) > 0)
      .select("ID", "RELATION_RELATIONS")
      .collect()
      .map(row => (row.getAs[Long]("ID"), row.getAs[Seq[Long]]("RELATION_RELATIONS")))
      .toMap

  }

  /**
   * Maps hierarchical relations and their top-most parent. The key is a relation id and value
   * will be it's parent. If relation id doesn't present as key in that index it could mean that relation either is not a
   * hierarchical relation or have no further parents. Relation may participate in several different hierarchies and, therefore,
   * have more than one parent.
   *
   * @param osm     OSM schema data frame.
   * @param members Map of relations descendants, provided by \see getRelationsDescendants
   * @return Map of relation IDs and their top-most parent's IDs
   */
  def makeRelationParentIndex(osm: DataFrame, members: Map[Long, Seq[Long]]): Map[Long, Seq[Long]] = {
    val invertedMembers = members.par
      .flatMap { case (rel, members) => members.map(member => (member, rel)) }
      .groupBy(_._1)
      .mapValues(_.values).seq.toMap

    val topParents = invertedMembers.mapValues(parents =>
      parents.flatMap(p => findNextParent(p, invertedMembers))
    )

    topParents.mapValues(_.seq.toList).seq
  }

  /**
   * Maps hierarchical relations and their kids. The key is a relation id nad value is a list of all it's
   * descendants, not just direct ones. If relation doesn't presents as a key in that index it means that relation does
   * not have any sub relations.
   *
   * @param osm     OSM schema data frame.
   * @param members Map of relations descendants, provided by \see getRelationsDescendants
   * @return Map od relation IDs and list of their kids.
   */
  def makeRelationChildrenIndex(osm: DataFrame, members: Map[Long, Seq[Long]]): Map[Long, Seq[Long]] = {
    val childrenWithParents = members.par.mapValues(kids =>
      kids.flatMap(kid => expandKids(kid, members))
    )

    childrenWithParents.seq.toMap
  }
}
