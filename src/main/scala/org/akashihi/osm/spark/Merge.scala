package org.akashihi.osm.spark

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
   * @param strict Duplicates objects removal policy.
   *               If set to 'true', only exactly matching objects will be removed.
   *               For example. if you are merging two datasets containing objects from different changes sets
   *               with same ID but different values for tags for example, strict removal policy will keep
   *               both objects in the resulting datasets.
   *               'false' value (default) applies relaxed policy - objects with matching ID and TYPE will be removed,
   *               even if other values of those objects are different.
   * @return
   */
  def apply(osm: Seq[DataFrame], strict: Boolean = false): DataFrame = {
    val merged = merge(osm.tail, osm.head)
    if (strict) {
      merged.distinct()
    } else {
      merged.dropDuplicates("ID", "TYPE")
    }
  }
}
