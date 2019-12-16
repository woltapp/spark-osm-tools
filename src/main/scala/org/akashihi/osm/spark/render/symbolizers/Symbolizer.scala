package org.akashihi.osm.spark.render.symbolizers

import java.awt.{AlphaComposite, Color, Graphics2D, RenderingHints}
import java.awt.image.BufferedImage

import com.vividsolutions.jts.geom.Coordinate
import javax.imageio.ImageIO
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.Row

import scala.util.Try
import collection.JavaConverters._

case class MapSymbol(minZoom: Int, layer: Integer, zorder: Integer, symbolizer: String, geometry: Seq[(Double, Double)], parameters: Map[String, String])

/**
 * Generic symbolizer definition.
 *
 * Supports 'color' and 'opacity' parameters.
 */
trait Symbolizer {
  protected val renderHints: Map[RenderingHints.Key, AnyRef] = Map(
    RenderingHints.KEY_ANTIALIASING -> RenderingHints.VALUE_ANTIALIAS_ON,
    RenderingHints.KEY_RENDERING -> RenderingHints.VALUE_RENDER_QUALITY,
    RenderingHints.KEY_ALPHA_INTERPOLATION -> RenderingHints.VALUE_ALPHA_INTERPOLATION_QUALITY,
    RenderingHints.KEY_COLOR_RENDERING -> RenderingHints.VALUE_COLOR_RENDER_QUALITY,
    RenderingHints.KEY_DITHERING -> RenderingHints.VALUE_DITHER_ENABLE,
    RenderingHints.KEY_FRACTIONALMETRICS -> RenderingHints.VALUE_FRACTIONALMETRICS_ON,
    RenderingHints.KEY_INTERPOLATION -> RenderingHints.VALUE_INTERPOLATION_BILINEAR,
    RenderingHints.KEY_STROKE_CONTROL -> RenderingHints.VALUE_STROKE_PURE

  )
  protected val quality = new RenderingHints(renderHints.asJava)

  protected def geometryPointsToCoordinates(geometry: Seq[(Long, Long)]): Seq[Coordinate] = geometry.map { case (x, y) => new Coordinate(x, y) }

  protected def tileSerialize(tile: BufferedImage): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    ImageIO.write(tile, "png", bos)
    bos.toByteArray
  }

  def rasterize(zoom: Int, geometry: Seq[(Long, Long)], parameters: Map[String, String]): Array[Byte] = {
    val tile = new BufferedImage(256, 256, BufferedImage.TYPE_INT_ARGB)
    val g2d = tile.createGraphics()
    val color = Color.decode("#" + parameters("color"))
    g2d.setColor(color)

    g2d.setRenderingHints(quality)

    val alpha = parameters.get("opacity").flatMap(o => Try(o.toFloat).toOption).getOrElse(1.0f)
    g2d.setComposite(AlphaComposite.getInstance(AlphaComposite.SRC_OVER, alpha))

    val drawn = rasterize(g2d, zoom, geometry, parameters)

    drawn.dispose()
    tileSerialize(tile)
  }

  def isRenderable(zoom: Int, geometry: Seq[(Long, Long)], parameters: Map[String, String]): Boolean

  def rasterize(g2d: Graphics2D, zoom: Int, geometry: Seq[(Long, Long)], parameters: Map[String, String]): Graphics2D
}

object Symbolizer {
  private val symbolizers = Map(
    "Polygon" -> new PolygonSymbolizer(),
    "Line" -> new LineSymbolizer(),
    "Text" -> new TextSymbolizer()
  )

  def isRenderable(symbolizer: String, zoom: Int, geometry: Seq[Row], parameters: Map[String, String]): Boolean = {
    val extractedRows = geometry.map(r => (r.getAs[Long](0), r.getAs[Long](1)))
    symbolizers(symbolizer).isRenderable(zoom, extractedRows, parameters)
  }

  def rasterize(symbolizer: String, zoom: Int, geometry: Seq[Row], parameters: Map[String, String]): Array[Byte] = {
    val extractedRows = geometry.map(r => (r.getAs[Long](0), r.getAs[Long](1)))

    symbolizers(symbolizer).rasterize(zoom, extractedRows, parameters)
  }
}
