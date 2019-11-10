package org.akashihi.osm.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.parallel.{ParIterable, ParSeq}

object Relation {
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
      .flatMap(v => if (v.nonEmpty) { Some(v) } else { None })
  }
  private def relationMemberIdByTypeUdf = udf(relationMemberIdByType _)

  private def findNextParent(id: Long, invertedMembers: Map[Long, ParIterable[Long]]):ParIterable[Long] = {
    if (invertedMembers.contains(id)) {
      invertedMembers(id).flatMap(p => findNextParent(p, invertedMembers))
    } else {
      ParSeq(id)
    }
  }

  /**
   * Adds relations member information to the dataframe.
   * Will add 3 new columns: RELATION_NODES, RELATION_WAYS, RELATION_RELATIONS
   * and fill them, for each relation, with the IDs of the relation members of the
   * corresponding type, preserving the order. For non relation entries or of relations without
   * some specific member type, values of those columns will be null.
   * @param osm OSM dataframe.
   * @return Same dataframe with RELATION_NODES, RELATION_WAYS and RELATION_RELATIONS columns
   */
  def makeRelationsContent(osm: DataFrame):DataFrame = {
    osm.withColumn("RELATION_NODES", relationMemberIdByTypeUdf(col("RELATION"), lit(OsmEntity.NODE)))
      .withColumn("RELATION_WAYS", relationMemberIdByTypeUdf(col("RELATION"), lit(OsmEntity.WAY)))
      .withColumn("RELATION_RELATIONS", relationMemberIdByTypeUdf(col("RELATION"), lit(OsmEntity.RELATION)))
  }

  /**
   * Maps hierarchical relations and their top-most parent. The key is a relation id and value
   * will be it's parent. If relation id doesn't present as key in that index it could mean that relation either is not a
   * hierarchical relation or have no further parents. Relation may participate in several different hierarchies and, therefore,
   * have more than one parent.
   * @param osm OSM schema data frame.
   * @return Map of relation IDs and their top-most parent's IDs
   */
  def makeRelationParentIndex(osm: DataFrame): Map[Long, Seq[Long]] = {
    val withContent = if (osm.schema.fields.map(_.name).contains("RELATION_RELATIONS")) {
      osm
    } else {
      makeRelationsContent(osm)
    }

    val members = withContent.filter(col("TYPE") === OsmEntity.RELATION)
      .filter(size(col("RELATION_RELATIONS"))>0)
      .select("ID", "RELATION_RELATIONS")
      .collect()
      .map(row => (row.getAs[Long]("ID"), row.getAs[Seq[Long]]("RELATION_RELATIONS")))
      .toMap

    val invertedMembers = members.par
      .flatMap { case (rel, members) => members.map(member => (member, rel)) }
      .groupBy(_._1)
      .mapValues(_.values).seq.toMap

    val topParents = invertedMembers.mapValues(parents =>
        parents.flatMap(p => findNextParent(p, invertedMembers))
    )

    topParents.mapValues(_.seq.toList).seq
  }
}
