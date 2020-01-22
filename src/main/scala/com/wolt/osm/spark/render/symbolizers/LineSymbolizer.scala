package com.wolt.osm.spark.render.symbolizers

import java.awt._

/**
 * Line drawing symbolizer.
 *
 * Supports 'cap', 'join', 'dash' parameters.
 */
class LineSymbolizer extends Symbolizer {
  private val capMap = Map(
    "butt" -> BasicStroke.CAP_BUTT,
    "round" -> BasicStroke.CAP_ROUND,
    "square" -> BasicStroke.CAP_SQUARE
  )
  private val joinMap = Map(
    "bevel" -> BasicStroke.JOIN_BEVEL,
    "miter" -> BasicStroke.JOIN_MITER,
    "round" -> BasicStroke.JOIN_ROUND
  )

  override def isRenderable(zoom: Int, geometry: Seq[(Long, Long)], parameters: Map[String, String]): Boolean = {
    geometry.distinct.size >= 2 // Minimal line is 2 points
  }

  override def rasterize(g2d: Graphics2D, zoom: Int, geometry: Seq[(Long, Long)], parameters: Map[String, String]): Graphics2D = {
    val primaryWidth = parameters.getOrElse("width", "1").toInt
    val adjustedWidth = primaryWidth - (18 - zoom)
    val width = if (adjustedWidth > 0) {
      adjustedWidth
    } else {
      1
    }
    val cap = capMap.getOrElse(parameters.getOrElse("cap", "butt"), BasicStroke.CAP_BUTT)
    val join = joinMap.getOrElse(parameters.getOrElse("join", "bevel"), BasicStroke.JOIN_BEVEL)
    val stroke = parameters.get("dash")
      .map(_.split(",").map(_.toFloat))
      .map(dash => new BasicStroke(width, cap, join, 1.0f, dash, 0.0f))
      .getOrElse(new BasicStroke(width, cap, join))
    g2d.setStroke(stroke)

    val x = geometry.map(_._1.toInt).toArray
    val y = geometry.map(_._2.toInt).toArray
    g2d.drawPolyline(x, y, geometry.size)
    g2d
  }
}
