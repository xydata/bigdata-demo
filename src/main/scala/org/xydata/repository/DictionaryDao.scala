package org.xydata.repository

/**
  * Created by iyunbo on 24/01/16.
  */
trait DictionaryDao {
  def fetch(): Map[String, Int]
}
