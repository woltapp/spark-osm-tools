package com.wolt.osm.spark

import org.apache.spark.sql._

import scala.annotation.tailrec

object Merge {
  @tailrec
  private def merge(osm: Seq[DataFrame], accumulator: DataFrame): DataFrame = {
    if (osm.isEmpty) {
      accumulator
    } else {
      merge(osm.tail, accumulator.union(osm.head))
    }
  }

  /**
   * Merges several OSM datasets into single one.
   *
   * @param osm Datasets to merge, shouldn't be empty.
   * @return Merged OSM dataset, without duplicate objects.
   */
  def apply(osm: Seq[DataFrame], strict: Boolean = false): DataFrame = {
    merge(osm.tail, osm.head).dropDuplicates("ID", "TYPE")
  }
}
