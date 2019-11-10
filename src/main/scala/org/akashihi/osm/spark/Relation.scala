package org.akashihi.osm.spark

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, udf}

import scala.collection.mutable

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
}
