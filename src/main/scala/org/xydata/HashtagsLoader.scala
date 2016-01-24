package org.xydata

import java.io.InputStream

import scala.io.Source
import scala.util.control.Breaks._

/**
  * Created by iyunbo on 24/01/16.
  */
object HashtagsLoader {
  //max number of hashtags allowed by Twitter streaming APIs filter
  private val MAX_NUM_TWITTER_HASHTAG = 400

  //Location of CSV file from where to read stocks
  private val CSV_LOCATION = "/securities.csv"

  //Delimitor used in CSV file
  private val FILE_DELIMITOR = ","

  def fetchHashtags(num: Int = 50): Array[String] = {
    val stream: InputStream = getClass.getResourceAsStream(CSV_LOCATION)
    var stockSymbolList = Array[String]()
    println("Reading stock symbols to send to Twitter from " + CSV_LOCATION)
    val src = Source.fromInputStream(stream)
    breakable {
      for (line <- src.getLines()) {
        val hashtag = line.split(FILE_DELIMITOR)(0)
        println("Adding to hashtag filters: " + hashtag)
        stockSymbolList :+= hashtag
        if (stockSymbolList.length >= num) {
          break
        }
      }
    }
    println("Total hashtags number: " + stockSymbolList.length)
    src.close
    stockSymbolList
  }
}
