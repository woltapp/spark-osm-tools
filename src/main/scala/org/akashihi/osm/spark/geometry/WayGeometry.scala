package org.akashihi.osm.spark.geometry

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object WayGeometry {
  /**
   * Converts ways defined as sequences of points id to the series of points coordinates.
   *
   * @param ways   Dataframe with 'WAY' column of type Seq[Long]. all other columns will be dropped
   * @param points Dataframe with the following columns:
   *               ID: Long - point ID
   *               LON: Double - point longitude
   *               LAT: Double - point latitude
   *               All the other columns will be ignored.
   * @return 'ways' dataset with 'geometry' column of type Seq[Seq[Double] ] where nested array consists of two values: lat and lon
   */
  def apply(ways: DataFrame, points: DataFrame): DataFrame = {
    val joinCoordinatesUdf = udf { (lon: Double, lat: Double) => Seq(lon, lat) }
    val joinMap = udf { values: Seq[Map[Long, Seq[Double]]] => values.flatten.toMap }
    val repairGeometry = udf { (points: Map[Long, Seq[Double]], nodes: Seq[Long]) => nodes.map(node => points(node)) }

    ways.select("WAY")
      .withColumn("POINT", explode(col("WAY")))
      .join(points.filter(col("TYPE") === 0).select("ID", "LAT", "LON"), col("POINT") === col("ID"), "leftouter")
      .drop("ID")
      .withColumn("COORD", joinCoordinatesUdf(col("LON"), col("LAT")))
      .drop("LAT", "LON")
      .groupBy("WAY").agg(collect_list(map(col("POINT"), col("COORD"))).as("COORDS"))
      .withColumn("GEOMETRY_MAP", joinMap(col("COORDS")))
      .drop("COORDS").cache()
      .withColumn("geometry", repairGeometry(col("GEOMETRY_MAP"), col("WAY")))
      .select("geometry")
  }
}
