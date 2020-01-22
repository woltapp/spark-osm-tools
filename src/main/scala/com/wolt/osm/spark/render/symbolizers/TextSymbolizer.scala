package com.wolt.osm.spark.render.symbolizers

import java.awt._
import java.awt.image.BufferedImage

/**
 * Text drawing symbolizer. Supports 'text' and 'font' perameters, both parameters are mandatory.
 */
class TextSymbolizer extends Symbolizer {
  override def isRenderable(zoom: Int, geometry: Seq[(Long, Long)], parameters: Map[String, String]): Boolean = {
    val font = new Font(parameters("font"), Font.PLAIN, 13)
    val img = new BufferedImage(1, 1, BufferedImage.TYPE_INT_ARGB)
    val g2d = img.getGraphics

    val metrics = g2d.getFontMetrics(font)
    val height = metrics.getHeight
    val width = metrics.stringWidth(parameters("text"))
    width <= 256 && height <= 256
  }

  override def rasterize(g2d: Graphics2D, zoom: Int, geometry: Seq[(Long, Long)], parameters: Map[String, String]): Graphics2D = {
    val font = new Font(parameters("font"), Font.PLAIN, 13)
    val metrics = g2d.getFontMetrics(font)
    val width = metrics.stringWidth(parameters("text"))
    val textX = geometry.head._1 - width / 2
    val textY = geometry.head._2

    g2d.setFont(font)
    g2d.drawString(parameters("text"), textX, textY)
    g2d
  }
}
