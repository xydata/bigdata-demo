package org.xydata.repository.impl

import java.io.InputStream

import com.typesafe.config.Config
import org.xydata.repository.HashtagsDao

import scala.io.Source
import scala.util.control.Breaks._

/**
  * Created by iyunbo on 24/01/16.
  */
class HashtagsCSVLoader(appConf: Config) extends HashtagsDao {
  lazy val hashtagsPath = appConf.getString("files.securities.path")
  //max number of hashtags allowed by Twitter streaming APIs filter
  private[repository] val MAX_NUM_TWITTER_HASHTAG = 400
  //Delimitor used in CSV file
  private val FILE_DELIMITOR = ","

  def fetch(num: Int = MAX_NUM_TWITTER_HASHTAG): Array[String] = {
    val stream: InputStream = getClass.getResourceAsStream(hashtagsPath)
    var stockSymbolList = Array[String]()
    println("Reading stock symbols to send to Twitter from " + hashtagsPath)
    val src = Source.fromInputStream(stream)
    breakable {
      for (line <- src.getLines()) {
        val hashtag = line.split(FILE_DELIMITOR)(0).toUpperCase
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
