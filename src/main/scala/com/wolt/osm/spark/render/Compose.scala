package com.wolt.osm.spark.render

import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream

import javax.imageio.ImageIO
import org.apache.commons.io.output.ByteArrayOutputStream

object Compose {
  def mergeLayer(layer: Seq[Array[Byte]]): Array[Byte] = {
    val outputTile = new BufferedImage(256, 256, BufferedImage.TYPE_INT_ARGB)
    val g2d = outputTile.createGraphics()

    layer.map(new ByteArrayInputStream(_))
      .map(ImageIO.read)
      .foreach(image => g2d.drawImage(image, 0, 0, 256, 256, null))

    g2d.dispose()
    val bos = new ByteArrayOutputStream()
    ImageIO.write(outputTile, "png", bos)
    bos.toByteArray
  }

  def mergeLayers(layers: Map[Int, Array[Byte]]): Array[Byte] = mergeLayer(layers.toSeq.sortBy(_._1).reverse.map(_._2))
}
