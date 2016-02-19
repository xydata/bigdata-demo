package org.xydata.repository

/**
  * Created by iyunbo on 24/01/16.
  */
trait HashtagsDao {
  def fetch(num: Int = 0): Array[String]
}
