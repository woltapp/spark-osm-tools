package org.akashihi.osm.spark.geometry

import org.akashihi.osm.spark.OsmEntity
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable

object ResolveMultipolygon {
  //TODO refactor and remove mutable lists
  def resolveRings(ways: Seq[Seq[Long]]): Seq[Seq[Long]] = {
    def formRing(candidates: mutable.ListBuffer[Seq[Long]]): Seq[Long] = {
      val ringFirstWay = candidates.head
      candidates.remove(0)
      //Check if it is already closed
      if (ringFirstWay.head == ringFirstWay.last) {
        return ringFirstWay
      }
      val ring = mutable.ListBuffer[Long]()
      ring ++= ringFirstWay

      while (candidates.nonEmpty) {
        val next = candidates.find(w => w.head == ring.last || w.last == ring.last)
        if (next.isEmpty) {
          //Broken polygon, stop processing
          return Seq.empty[Long]
        }
        val nextWay = next.get
        candidates.remove(candidates.indexOf(nextWay))
        val direcitoned = if (nextWay.head == ring.last) {
          nextWay
        } else {
          nextWay.reverse
        }
        ring ++= direcitoned.tail
        if (ring.head == ring.last) {
          //Ok, we got ring
          return ring
        }
      }
      Seq.empty[Long]
    }
    val rings = mutable.ListBuffer[Seq[Long]]()
    val mutableWays = ways.to[mutable.ListBuffer]
    while (mutableWays.nonEmpty) {
      val nextRing = formRing(mutableWays)
      if (nextRing.isEmpty) {
        //Ring forming failed, return empty reuslt
        return Seq.empty[Seq[Long]]
      }
      rings += nextRing
    }
    rings
  }

  /**
   * Converts multipolygon definition to the list of outer/inner rings
   * @param relations List of multipolygon relations to work on. Should follow OSM schema.
   * @param osm OSM database with points and ways dfinitions.
   * @return Dataframe with following columns:
   *         geometry: Seq[Seq[Double] ] polygon geometry definition, where nested array consists of exactly two values: lat and lon.
   *         ID: Long - Owning relation ID.
   *         ROLE: String - outer or inner role of the polygon.
   */
  def apply(relations: DataFrame, osm: DataFrame): DataFrame = {
    val relDefs = relations.select("ID", "RELATION")
      .withColumn("MEMBER", explode(col("RELATION")))
      .filter(col("MEMBER")("TYPE") === OsmEntity.WAY)
      .drop("RELATION")
      .withColumn("WAY_ID", col("MEMBER")("ID"))
      .withColumn("ROLE", col("MEMBER")("ROLE"))
      .na.fill("outer", Seq("ROLE"))
      .drop("MEMBER")

    val ways = osm.filter(col("TYPE") === OsmEntity.WAY)
        .select("ID", "WAY")
      .withColumnRenamed("ID", "W_ID")

    val relWays = relDefs.join(ways, col("W_ID") === col("WAY_ID"), "leftouter")
        .drop("W_ID", "WAY_ID")
        .groupBy(col("ID"), col("ROLE")).agg(collect_list(col("WAY")).as("WAYS"))

    val resolveRingsUdf = udf(resolveRings _)
    val rings = relWays.withColumn("RINGS", resolveRingsUdf(col("WAYS")))
        .drop("WAYS")
        .withColumn("WAY", explode(col("RINGS")))
        .drop("RINGS")

    WayGeometry(rings, osm).withColumnRenamed("WAY", "GEO_WAY")
        .join(rings, col("WAY") === col("GEO_WAY"), "inner")
        .drop("WAY", "GEO_WAY")
  }
}
