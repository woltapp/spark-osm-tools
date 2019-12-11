import org.akashihi.osm.spark.{Extract, OsmEntity}
import org.akashihi.osm.spark.OsmSource.OsmSource
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom.GeometryFactory

object PTCoverage {
  private val geometryFactory = new GeometryFactory()

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
    val lon = geometry.map(_.head).sum/geometry.size.toDouble
    val lat = geometry.map(_.last).sum/geometry.size.toDouble
    Seq(lon, lat)
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
    val area = Extract(osm, 16.578809, 49.212551, 16.595750, 49.205591, Extract.CompleteRelations, spark).persist(StorageLevel.MEMORY_AND_DISK)

    //Get stops
    val stop_positions = area.filter(col("TYPE") === OsmEntity.NODE).filter(lower(col("TAG")("public_transport")) === "stop_position").select("LON", "LAT")
      .collect().map(row => (row.getAs[Double]("LON"),row.getAs[Double]("LAT")))

    //Get buildings
    val way_buildings = area.filter(col("TYPE") === OsmEntity.WAY).filter(lower(col("TAG")("building")).isNotNull)
      .filter(col("TAG")("building:ruian:type").isin("6", "7", "8", "11")) //Only valid for Czech Republic, remove filtering for other countries
      .select("WAY")
      .filter(size(col("WAY")) > 2)
      .filter(col("WAY")(0) === col("WAY")(size(col("WAY")) - 1))
    //TODO Add support for multipolygon buildings
    val buildings = way_buildings.persist(StorageLevel.MEMORY_AND_DISK)

    //Converts buildings to geometry
    val joinCoordinatesUdf = udf { (lon: Double, lat: Double) => Seq(lon, lat) }
    val joinMap = udf { values: Seq[Map[Long, Seq[Double]]] => values.flatten.toMap }
    val repairGeometry = udf { (points: Map[Long, Seq[Double]], nodes: Seq[Long]) => nodes.map(node => points(node)) }
    val points = area.filter(col("TYPE") === 0).select("ID", "LAT", "LON").cache()
    val buildingsGeometry = buildings.withColumn("POINT", explode(col("WAY")))
      .join(points.alias("NODES_F"), col("POINT") === col("NODES_F.ID"), "leftouter")
      .drop("ID")
      .withColumn("COORD", joinCoordinatesUdf(col("LON"), col("LAT")))
      .drop("LAT", "LON")
      .groupBy("WAY").agg(collect_list(map(col("POINT"), col("COORD"))).as("COORDS"))
      .withColumn("GEOMETRY_MAP", joinMap(col("COORDS")))
      .drop("COORDS").cache()
      .withColumn("geometry", repairGeometry(col("GEOMETRY_MAP"), col("WAY")))
      .select("geometry")

    //Find buildings mean points
    val meanPointUdf = udf (meanPoint _)
    val buildingsMeanPoints = buildingsGeometry.withColumn("MEAN_POINT", meanPointUdf(col("geometry")))

    //Find distance to the nearest public transport stop
    val distanceUdf = udf{ (lon: Double, lat: Double) => stop_positions.map(position => haversine(lon, lat, position._1, position._2)).min}
    val buildingsWithDistances = buildingsMeanPoints.withColumn("DISTANCE", distanceUdf(col("MEAN_POINT")(0), col("MEAN_POINT")(1)))
        .drop("MEAN_POINT")
    //buildingsWithDistances.show(false)
    buildingsWithDistances.agg(max(col("DISTANCE"))).show(false)
  }
}
