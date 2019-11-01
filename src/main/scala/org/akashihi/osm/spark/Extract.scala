package org.akashihi.osm.spark

import org.apache.spark.sql.DataFrame
import org.locationtech.jts.geom._

object Extract {
  private val factory = new GeometryFactory()

  def apply(osm: DataFrame, areas: Seq[Envelope])= Extract.apply(osm, areas.map(e => factory.createPolygon(factory.toGeometry(e).getCoordinates)))
  def apply(osm: DataFrame, areas: Seq[Polygon])= {

  }
}
