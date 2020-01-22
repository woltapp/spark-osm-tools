import com.wolt.osm.spark.OsmSource.OsmSource
import com.wolt.osm.spark.render.Renderer
import com.wolt.osm.spark.geometry.{ResolveMultipolygon, WayGeometry}
import com.wolt.osm.spark.{Extract, OsmEntity}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object PTCoverage {
  private def haversine(startLon: Double, startLat: Double, endLon: Double, endLat: Double): Double = {
    val R = 6378137d
    val dLat = math.toRadians(endLat - startLat)
    val dLon = math.toRadians(endLon - startLon)
    val lat1 = math.toRadians(startLat)
    val lat2 = math.toRadians(endLat)

    val a =
      math.sin(dLat / 2) * math.sin(dLat / 2) +
        math.sin(dLon / 2) * math.sin(dLon / 2) * math.cos(lat1) * math.cos(lat2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    R * c
  }

  private def meanPoint(geometry: Seq[Seq[Double]]): Seq[Double] = {
    val lon = geometry.map(_.head).sum / geometry.size.toDouble
    val lat = geometry.map(_.last).sum / geometry.size.toDouble
    Seq(lon, lat)
  }

  private def distanceToRenderParameters(distance: Double): Map[String, String] = {
    val distanceMap = Seq(
      0 -> "00aaff",
      100 -> "00ffff",
      200 -> "00ffaa",
      300 -> "00ff55",
      400 -> "1aff00",
      500 -> "88ff00",
      600 -> "aaff00",
      700 -> "BFFF00",
      800 -> "d0ff00",
      900 -> "ffff00",
      1000 -> "ff8800",
      1500 -> "FF5100",
      2000 -> "ff0000"
    )
    val color = distanceMap.filter(_._1 < distance).lastOption.map(_._2).getOrElse("0000ff")
    Map("fill" -> "true", "color" -> color, "opacity" -> "0.5")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("PublicTransportCoverage")
      .config("spark.master", "local[12]")
      .config("spark.executor.memory", "16gb")
      .getOrCreate()

    //Load da map
    val osm = spark.read
      .option("threads", 2)
      .option("partitions", 12)
      .format(OsmSource.OSM_SOURCE_NAME)
      .load("/home/chollya/maps/cities/extract-brno.osm.pbf").drop("INFO")
      .persist(StorageLevel.MEMORY_AND_DISK)

    //Cut to the interest area
    val zabovreskyBoundary = osm.filter(col("TYPE") === OsmEntity.RELATION)
      //.filter(lower(col("TAG")("boundary")) === "administrative" && col("TAG")("admin_level") === "10" && col("TAG")("ref") === "610470") //Zabovresky
      .filter(lower(col("TAG")("boundary")) === "administrative" && col("TAG")("admin_level") === "8" && col("TAG")("ref") === "CZ0642582786") //Brno
    val zabovreskyPolygon = ResolveMultipolygon(zabovreskyBoundary, osm).select("geometry").first().getAs[Seq[Seq[Double]]]("geometry")
    val area = Extract(osm, zabovreskyPolygon, Extract.CompleteRelations, spark).persist(StorageLevel.MEMORY_AND_DISK)

    //Get stops
    val stop_positions = area.filter(col("TYPE") === OsmEntity.NODE).filter(lower(col("TAG")("public_transport")) === "stop_position").select("LON", "LAT")
      .collect().map(row => (row.getAs[Double]("LON"), row.getAs[Double]("LAT")))

    //Get buildings
    val way_buildings = area.filter(col("TYPE") === OsmEntity.WAY).filter(lower(col("TAG")("building")).isNotNull)
      .filter(col("TAG")("building:ruian:type").isin("6", "7", "8", "11") || col("TAG")("building:ruian:type").isNull) //Only valid for Czech Republic, remove filtering for other countries
      .select("WAY")
      .filter(size(col("WAY")) > 2)
      .filter(col("WAY")(0) === col("WAY")(size(col("WAY")) - 1))
    //TODO Add support for multipolygon buildings
    val buildings = way_buildings.persist(StorageLevel.MEMORY_AND_DISK)

    //Converts buildings to geometry
    val buildingsGeometry = WayGeometry(buildings, area).drop("WAY")

    //Find buildings mean points
    val meanPointUdf = udf(meanPoint _)
    val buildingsMeanPoints = buildingsGeometry.withColumn("MEAN_POINT", meanPointUdf(col("geometry")))

    //Find distance to the nearest public transport stop
    val distanceUdf = udf { (lon: Double, lat: Double) => stop_positions.map(position => haversine(lon, lat, position._1, position._2)).min }
    val buildingsWithDistances = buildingsMeanPoints.withColumn("DISTANCE", distanceUdf(col("MEAN_POINT")(0), col("MEAN_POINT")(1)))
      .drop("MEAN_POINT")

    //Symbolize geometry for rendering
    val distanceToRenderParametersUdf = udf(distanceToRenderParameters _)
    val symbolized = buildingsWithDistances.withColumn("symbolizer", lit("Polygon")) //Render buildings as polygons
      .withColumn("layer", lit(0)) //The top most layer
      .withColumn("zorder", lit(0)) //And top most sub-layer
      .withColumn("minZoom", lit(13)) //Render only at zoom levels starting from 13
      .withColumn("parameters", distanceToRenderParametersUdf(col("DISTANCE")))

    //Send it to the rendering pipeline
    Renderer(symbolized, 13 to 19, "/home/chollya/tiles/public_transport_coverage")
  }
}
