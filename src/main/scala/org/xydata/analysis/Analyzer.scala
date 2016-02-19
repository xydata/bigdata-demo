package org.xydata.analysis

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
trait Analyzer[M] extends Serializable {
  def analyze(content: M)
}
