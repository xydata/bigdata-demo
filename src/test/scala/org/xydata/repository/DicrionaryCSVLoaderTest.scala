package org.xydata.repository

import org.scalatest._
import org.xydata.twitter.TweetModule

/**
  * Created by iyunbo on 24/01/16.
  */
class DicrionaryCSVLoaderTest extends FlatSpec with Matchers with TweetModule {
  "DicrionaryCSVLoader" should "load all words from the csv file" in {
    val words = dictionaryDao.fetch()
    words.size should be > 10
    words("good") should be(1)
    words("bad") should be(-1)
  }
}
