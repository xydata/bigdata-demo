package org.xydata.repository

import org.scalatest._
import org.xydata.twitter.TweetModule

/**
  * Created by iyunbo on 24/01/16.
  */
class HashtagsCSVLoaderTest extends FlatSpec with Matchers with TweetModule {
  "HashtagsCSVLoader" should "load all stock symbols from the csv file" in {
    val hashtags = hashtagsDao.fetch()
    hashtags.length should be(400)
  }
}
