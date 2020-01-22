package com.wolt.osm.spark.render

import java.io.{File, FileOutputStream}

import com.wolt.osm.spark.render.symbolizers.Symbolizer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * Rendering pipeline.
 */
object Renderer {
  /**
   * Web mercator projection
   *
   * @param zoom Projection zoom level.
   * @param lat  Latitude in degrees of the point to project.
   *             Must be in range -85.0511 - 85.0511
   * @param lon  Longitude in degress of the point to project.
   * @return (X,Y) projected point coordinates.
   */
  def webMercator(zoom: Int)(lat: Double, lon: Double): (Long, Long) = {
    val multiplier = 256.0 / (Math.PI * 2) * Math.pow(2, zoom)

    val x = multiplier * (Math.toRadians(lon) + Math.PI)
    val y = multiplier * (Math.PI - Math.log(Math.tan(Math.PI / 4 + Math.toRadians(lat) / 2)))
    (x.round, y.round)
  }

  /**
   * Checks if geometry could be projected using web mercator
   *
   * @param point Sequence of points, forming geometry.
   * @return Tru if geometry can be projected, false otherwise
   */
  def webMercatorValidGeometry(point: Seq[Seq[Double]]): Boolean = point.forall(point => point.last >= -85.0511 && point.last <= 85.0511 && point.head >= -180 && point.head <= 180)

  /**
   * Projects point to the web mercator plane using specified zoom level
   *
   * @param zoom Zoom level
   * @param r    Point wrapped into row of lon and lat as double
   * @return (X,Y) Point coordinates projected to the zoom plane
   */
  private def pointProject(zoom: Int, r: Seq[Double]): Option[(Long, Long)] = Option(r).map { p => webMercator(zoom)(p.last, p.head) }

  /**
   * Projects geometry points to the web mercator plane using specified zoom level
   *
   * @param zoom Zoom level
   * @param g    Sequence of geometry points wrapped into rows of lon and lat as double
   * @return Sequence of (X,Y) point coordinates projected to the zoom plane ordered same way as g Sequence
   */
  private def geometryProject(zoom: Int, g: Seq[Seq[Double]]): Seq[(Long, Long)] = {
    val points = g.map(p => pointProject(zoom, p))
    if (points.exists(_.isEmpty)) {
      Seq[(Long, Long)]()
    } else {
      points.flatten
    }
  }

  def findTilesRangeForGeometry(geometry: Seq[Row]): (Seq[Long], Seq[Long]) = {
    val xAxis = geometry.map(_.getAs[Long]("_1"))
    val yAxis = geometry.map(_.getAs[Long]("_2"))

    val xMinTile = xAxis.min / 256
    val xMaxTile = xAxis.max / 256

    val xTiles = xMinTile to xMaxTile
    val yMinTile = yAxis.min / 256
    val yMaxTile = yAxis.max / 256

    val yTiles = yMinTile to yMaxTile

    xTiles -> yTiles
  }

  /**
   * Shift coordinates of a point to tile space.
   * Tile plane is a square of size [0;2&#94;zoom] consisting
   * of 2&#94;zoom/128 tiles. Each tile is a square of 256 to 256
   * pizels. This function transfer point coordinates from tile plane
   * space to specific tile space. Coordinates may NOT fit the tile,
   * in that case they will be out of [0; 256] range.
   *
   * @param tileX X coordinate of a tile in a tile plane (tile number)
   * @param tileY Y coordinate of a tile in a tile plane (tile number)
   * @param x     X coordinate of a point
   * @param y     Y coordinate of a point
   * @return (X,Y) Point coordinates in a tile space.
   */
  private def shiftToTile(tileX: Long, tileY: Long, x: Long, y: Long): (Long, Long) = {
    val tileXShift = tileX * 256
    val tileYshift = tileY * 256
    (x - tileXShift, y - tileYshift)
  }

  private def geometryShiftToTile(tileX: Long, tileY: Long, geometry: Seq[Row]): Seq[(Long, Long)] = geometry.map(r => shiftToTile(tileX, tileY, r.getAs[Long](0), r.getAs[Long](1)))

  /**
   * Render call. Will produce tiles for specified zoom levels, covering data in synmbols and write
   * them to the target directory in zoom_level/y_tile/x_tile format
   *
   * @param symbols    DataFrame of MapSymbols objects or similar
   * @param zoomLevels List of zoom levels to render, positive integers only. Each zoom level is two times bigger, comparing to previous one.
   * @param target     Drectory where tiles should be written.
   */
  def apply(symbols: DataFrame, zoomLevels: Seq[Int], target: String): Unit = {

    // Drop non-projectable geometry
    val geometryValidator = udf(webMercatorValidGeometry _)
    val validGeometry = symbols.filter(geometryValidator(col("geometry")))

    // Add zoom levels and project
    val geometryProjectUdf = udf(geometryProject _)
    val isRenderableUdf = udf(Symbolizer.isRenderable _)
    val projected = validGeometry
      .withColumn("ZOOM", explode(lit(zoomLevels.toArray)))
      .filter(col("ZOOM") >= col("minZoom"))
      .withColumn("PROJECTED", geometryProjectUdf(col("ZOOM"), col("geometry")))
      .filter(isRenderableUdf(col("symbolizer"), col("ZOOM"), col("PROJECTED"), col("parameters"))) //Drop too small objects

    // Split to tiles
    val findTilesRangeForGeometryUdf = udf(findTilesRangeForGeometry _)
    val geometryShiftToTileUdf = udf(geometryShiftToTile _)
    val tiles = projected.withColumn("TILES", findTilesRangeForGeometryUdf(col("PROJECTED")))
      .withColumn("TILE_X", explode(col("TILES")("_1")))
      .withColumn("TILE_Y", explode(col("TILES")("_2")))
      .withColumn("PROJECTED_TILE", geometryShiftToTileUdf(col("TILE_X"), col("TILE_Y"), col("PROJECTED")))
      .drop("PROJECTED", "TILES")

    // Rasterize map features
    val rasterizeUdf = udf(Symbolizer.rasterize _)
    val rendered = tiles.withColumn("RASTER", rasterizeUdf(col("symbolizer"), col("ZOOM"), col("PROJECTED_TILE"), col("parameters")))
      .drop("PROJECTED_TILE", "parameters", "symbolizer")

    // Merge layer features by z-order
    val mergeLayerUdf = udf(Compose.mergeLayer _)
    val mergeLayersUdf = udf(Compose.mergeLayers _)
    val joinMap = udf { values: Seq[Map[Int, Array[Byte]]] => values.flatten.toMap }
    val zordered = rendered
      .groupBy("layer", "zorder", "ZOOM", "TILE_X", "TILE_Y").agg(collect_list(col("RASTER")).as("MERGED")).drop("RASTER")
      .withColumn("RASTER", mergeLayerUdf(col("MERGED"))).drop("MERGED")
      .groupBy("layer", "ZOOM", "TILE_X", "TILE_Y").agg(collect_list(map(col("zorder"), col("RASTER"))).as("MERGED"))
      .withColumn("LAYERS", joinMap(col("MERGED"))).drop("MERGED")
      .withColumn("RASTER", mergeLayersUdf(col("LAYERS"))).drop("LAYERS")

    // Merge layer features
    val merged = zordered.groupBy("layer", "ZOOM", "TILE_X", "TILE_Y").agg(collect_list(col("RASTER")).as("MERGED")).drop("RASTER")
      .withColumn("RASTER", mergeLayerUdf(col("MERGED"))).drop("MERGED")

    // Merge layers
    val images = merged.drop("dimensions").groupBy("ZOOM", "TILE_X", "TILE_Y").agg(collect_list(map(col("layer"), col("RASTER"))).as("MERGED"))
      .withColumn("LAYERS", joinMap(col("MERGED"))).drop("MERGED")
      .withColumn("TILE", mergeLayersUdf(col("LAYERS"))).drop("LAYERS")

    // Write da images
    images.select("ZOOM", "TILE_X", "TILE_Y", "TILE")
      .foreach(row => {
        val fname = s"$target/${row.getAs[Int]("ZOOM")}/${row.getAs[Int]("TILE_X")}/${row.getAs[Int]("TILE_Y")}.png"
        val file = new File(fname)
        file.getParentFile.mkdirs()
        val output = new FileOutputStream(fname)
        output.write(row.getAs[Array[Byte]]("TILE"))
        output.close()
      })

  }
}
