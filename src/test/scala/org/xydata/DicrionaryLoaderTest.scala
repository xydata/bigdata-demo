package org.xydata

import org.scalatest._
import org.xydata.util.DictionaryLoader

/**
  * Created by iyunbo on 24/01/16.
  */
class DicrionaryLoaderTest extends FlatSpec with Matchers {
  "DicrionaryLoader" should "load all words from the csv file" in {
    val words = DictionaryLoader.fetchWords()
    words.size should be > 10
    words("good") should be(1)
    words("bad") should be(-1)
  }
}
