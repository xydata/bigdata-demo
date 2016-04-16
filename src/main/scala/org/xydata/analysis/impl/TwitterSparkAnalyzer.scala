package org.xydata.analysis.impl

import com.typesafe.config.Config
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.dstream.DStream
import org.xydata.analysis.Analyzer
import org.xydata.avro.Status

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
class TwitterSparkAnalyzer(streamWindow: Int, dictionary: Map[String, Int], hashtags: Array[String]) extends Analyzer[DStream[Status]] {

  override def analyze(tweets: DStream[Status]): Unit = {

    val scores = tweets
      // split into phrases
      .flatMap(t => t.getText.split("\\.\\s"))
      // calculate scores
      .flatMap(t => scoring(t))
      .reduceByKeyAndWindow(
        (v1, v2) => reduceScore(v1, v2), Milliseconds(streamWindow)
      )

    val scoreSorted = scores.transform(_.sortBy(_._2._2, ascending = false))

    scoreSorted.print()
  }

  def reduceScore(v1: (Int, Int), v2: (Int, Int)): (Int, Int) = {
    (v1._1 + v2._1, v1._2 + v2._2)
  }

  def scoring(phrase: String): Seq[(String, (Int, Int))] = {
    val words = phrase.split(" ")
    val score = words.filter(w => dictionary.contains(w.toLowerCase()))
      .map(w => dictionary.get(w.toLowerCase))
      .foldLeft(Some(0))((a, b) => Some(a.getOrElse(0) + b.getOrElse(0)))
    val matchedHashtags = words.filter(w => hashtags.contains(w.toUpperCase))
    var scoreMap = Seq[(String, (Int, Int))]()
    matchedHashtags.foreach((tag) => scoreMap = scoreMap :+(tag.toUpperCase, (score.get, 1)))
    scoreMap
  }

}
