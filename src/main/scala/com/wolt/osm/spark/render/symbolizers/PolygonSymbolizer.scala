package com.wolt.osm.spark.render.symbolizers

import java.awt.Graphics2D

/**
 * Polygon symbolizer. Support 'fill' parameter.
 */
class PolygonSymbolizer extends Symbolizer {
  override def isRenderable(zoom: Int, geometry: Seq[(Long, Long)], parameters: Map[String, String]): Boolean = {
    geometry.distinct.size >= 3 //Minimal polygon is a triangle
  }

  override def rasterize(g2d: Graphics2D, zoom: Int, geometry: Seq[(Long, Long)], parameters: Map[String, String]): Graphics2D = {
    val polygon = new java.awt.Polygon()
    geometry.foreach { case (x, y) => polygon.addPoint(x.toInt, y.toInt) }

    val fill = parameters.get("fill").exists(_.equalsIgnoreCase("true"))
    if (fill) {
      g2d.fillPolygon(polygon)
    } else {
      g2d.drawPolygon(polygon)
    }
    g2d
  }
}
