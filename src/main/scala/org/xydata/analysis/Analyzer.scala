package org.xydata.analysis

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
trait Analyzer[M] extends Serializable {
  def reduceScore(tuple1: (Int, Int), tuple2: (Int, Int)): (Int, Int)

  def scoring(s: String): Seq[(String, (Int, Int))]

  def analyze(content: M)
}
