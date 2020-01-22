package com.wolt.osm.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.jts.geom.Envelope

object BoundBox {
  def findBBox(osm: DataFrame): Envelope = {
    val bounds = osm.filter(col("LON").isNotNull.and(col("LAT").isNotNull))
      .select("LON", "LAT", "TYPE")
      .groupBy("TYPE")
      .agg(min("LAT").as("MIN_LAT"),
        min("LON").as("MIN_LON"),
        max("LAT").as("MAX_LAT"),
        max("LON").as("MAX_LON"))
      .first()

    val left = bounds.getAs[Double]("MIN_LAT")
    val top = bounds.getAs[Double]("MAX_LON")
    val right = bounds.getAs[Double]("MAX_LAT")
    val bottom = bounds.getAs[Double]("MIN_LON")
    new Envelope(left, right, bottom, top)
  }
}
