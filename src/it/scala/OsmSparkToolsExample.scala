import org.akashihi.osm.spark.OsmSource.OsmSource
import org.akashihi.osm.spark.{BoundBox, Extract, Merge}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object OsmSparkToolsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OsmSparkTools")
      .config("spark.master", "local[24]")
      .config("spark.executor.memory", "4gb")
      .getOrCreate()

    val osmFiles = args(0).split(",").map(filename => spark.read
      .option("threads", 6)
      .option("partitions", 8)
      .format(OsmSource.OSM_SOURCE_NAME)
      .load(filename).drop("INFO")
      .persist(StorageLevel.MEMORY_AND_DISK))

    println("Input files bboxes:")
    osmFiles.map(BoundBox.findBBox).foreach(println)

    val merged = Merge(osmFiles).persist(StorageLevel.MEMORY_AND_DISK)

    println(s"Merged osm data bbox: ${BoundBox.findBBox(merged)}")

    val extracted = Extract(spark, merged, 16.578809,49.212551, 16.595750, 49.205591)

    println(s"Extracted osm data bbox: ${BoundBox.findBBox(extracted)}")
  }
}
