import org.akashihi.osm.spark.{BoundBox, Merge}
import org.akashihi.osm.spark.OsmSource.OsmSource
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

object OsmSparkToolsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OsmSparkTools")
      .config("spark.master", "local[4]")
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

    val merged = Merge(osmFiles)

    println(s"Merged osm data bbox: ${BoundBox.findBBox(merged)}")
  }
}
