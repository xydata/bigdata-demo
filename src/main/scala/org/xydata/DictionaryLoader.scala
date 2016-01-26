package org.xydata

import java.io.InputStream

import scala.io.Source
import scala.util.control.Breaks._

/**
  * Created by iyunbo on 24/01/16.
  */
object DictionaryLoader {

  //Location of CSV file from where to read words
  private val CSV_LOCATION = "/dictionary.csv"

  //Delimitor used in CSV file
  private val FILE_DELIMITOR = ","

  def fetchWords(): Map[String, Int] = {
    val stream: InputStream = getClass.getResourceAsStream(CSV_LOCATION)
    var words = Map[String, Int]()
    println("Reading dictionary from " + CSV_LOCATION)
    val src = Source.fromInputStream(stream)
    breakable {
      for (line <- src.getLines()) {
        val cols: Array[String] = line.split(FILE_DELIMITOR)
        val word = cols(0).toLowerCase
        val score = cols(1) match {
          case "positive" => 1
          case "negative" => -1
          case _ => 0
        }
        words += word -> score
      }
    }
    println("Total words number: " + words.size)
    src.close
    words
  }
}
