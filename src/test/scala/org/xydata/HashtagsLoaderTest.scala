package org.xydata

import org.scalatest._
import org.xydata.util.HashtagsLoader

/**
  * Created by iyunbo on 24/01/16.
  */
class HashtagsLoaderTest extends FlatSpec with Matchers {
  "HashtagsLoader" should "load all stock symbols from the csv file" in {
    val hashtags = HashtagsLoader.fetchHashtags(100)
    hashtags.length should be(100)
  }
}
