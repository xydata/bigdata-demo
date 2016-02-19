package org.xydata.repository.impl

import java.io.InputStream

import com.typesafe.config.Config
import org.xydata.repository.DictionaryDao

import scala.io.Source
import scala.util.control.Breaks._

/**
  * Created by iyunbo on 24/01/16.
  */
class DictionaryCSVLoader(appConf: Config) extends DictionaryDao {

  lazy val dictionaryPath = appConf.getString("files.dictionary.path")
  //Delimitor used in CSV file
  private val FILE_DELIMITOR = ","

  def fetch(): Map[String, Int] = {
    val stream: InputStream = getClass.getResourceAsStream(dictionaryPath)
    var words = Map[String, Int]()
    println("Reading dictionary from " + dictionaryPath)
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
