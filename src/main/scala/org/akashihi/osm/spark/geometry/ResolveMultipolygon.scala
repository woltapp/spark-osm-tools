package org.akashihi.osm.spark.geometry

import org.akashihi.osm.spark.OsmEntity
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable

object ResolveMultipolygon {
  def resolveRings(ways: Seq[Seq[Long]]): Seq[Seq[Long]] = {
    def formRing(cadidates: mutable.Map[Long, Seq[Seq[Long]]]): Seq[Long] = {
      val ringFirstWay = cadidates.filter {case (_,v) => v.size == 1}.head
      val ring = mutable.ListBuffer[Long]()
      ring ++= ringFirstWay._2.head
      cadidates.remove(ringFirstWay._1)

      while (cadidates.nonEmpty) {
        val next = cadidates.get(ring.last)
        if (next.isEmpty) {
          //Broken polygon, stop processing
          return Seq.empty[Long]
        }
        val nextWays = next.get
        if (nextWays.size >2 ) {
          //Broken polygon, stop processing
          return Seq.empty[Long]
        }
        if (nextWays.size == 2) {
          cadidates.update(ring.last, Seq(nextWays.last))
        } else {
          cadidates.remove(ring.last)
        }
        val nextWay = nextWays.head
        ring ++= nextWay
        if (ring.head == ring.last) {
          //Ok, we got ring
          return ring
        }
      }
      Seq.empty[Long]
    }
    val rings = mutable.ListBuffer[Seq[Long]]()
    val waysMap = mutable.Map(ways.groupBy(_.head).toSeq: _*)
    while (waysMap.nonEmpty) {
      val nextRing = formRing(waysMap)
      if (nextRing.isEmpty) {
        //Ring forming failed, return empty reuslt
        return Seq.empty[Seq[Long]]
      }
      rings += nextRing
    }
    rings
  }
  def apply(relations: DataFrame, osm: DataFrame): DataFrame = {
    val relDefs = relations.select("ID", "RELATION")
      .withColumn("MEMBER", explode(col("RELATION")))
      .filter(col("MEMBER")("TYPE") === OsmEntity.WAY)
      .drop("RELATION")
      .withColumn("WAY_ID", col("MEMBER")("ID"))
      .withColumn("WAY_ROLE", col("MEMBER")("ROLE"))
      .na.fill("outer", Seq("WAY_ROLE"))
      .drop("MEMBER")

    val ways = osm.filter(col("TYPE") === OsmEntity.WAY)
        .select("ID", "WAY")
      .withColumnRenamed("ID", "W_ID")

    val relWays = relDefs.join(ways, col("W_ID") === col("WAY_ID"), "leftouter")
        .drop("W_ID", "WAY_ID")
        .groupBy(col("ID"), col("WAY_ROLE")).agg(collect_list(col("WAY")).as("WAYS"))

    val resolveRingsUdf = udf(resolveRings _)
    val rings = relWays.withColumn("RINGS", resolveRingsUdf(col("WAYS")))
        .drop("WAYS")
    rings.show(false)
    rings
  }
}
