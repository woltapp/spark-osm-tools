package org.akashihi.osm.spark

import org.akashihi.osm.parallelpbf.entity.BoundBox
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object BoundBox {
  def findBBox(osm: DataFrame): BoundBox = {
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
    new BoundBox(left, top, right, bottom)
  }
}
