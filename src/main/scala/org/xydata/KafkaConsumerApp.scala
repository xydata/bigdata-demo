package org.xydata

import com.twitter.bijection.avro.SpecificAvroCodecs
import com.typesafe.config.ConfigFactory
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.xydata.avro.Tweet

object KafkaConsumerApp extends App {

  private val conf = ConfigFactory.load()

  val sparkConf = new SparkConf().setAppName("bigdata-demo").setMaster("local[*]")
  val sc = new StreamingContext(sparkConf, Seconds(5))

  val encTweets = {
    val topics = Map(KafkaProducerApp.KafkaTopic -> 1)
    val kafkaParams = Map(
      "zookeeper.connect" -> conf.getString("kafka.zookeeper.quorum"),
      "group.id" -> "1")
    KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      sc, kafkaParams, topics, StorageLevel.MEMORY_ONLY)
  }

  val dictionary = DictionaryLoader.fetchWords()
  val hashtags = HashtagsLoader.fetchHashtags()

  val tweets = encTweets.flatMap(x => SpecificAvroCodecs.toBinary[Tweet].invert(x._2).toOption)
  val scores = tweets.flatMap(t => scoring(t)).reduceByKeyAndWindow((v1, v2) => add(v1, v2), Seconds(30))

  def add(v1: (Int, Int), v2: (Int, Int)): (Int, Int) = {
    (v1._1 + v2._1, v1._2 + v2._2)
  }

  def scoring(t: Tweet): Seq[(String, (Int, Int))] = {
    val words = t.getText.split(" ")
    val score = words.filter(w => dictionary.contains(w.toLowerCase()))
      .map(w => dictionary.get(w.toLowerCase))
      .foldLeft(Some(0))((a, b) => Some(a.getOrElse(0) + b.getOrElse(0)))
    val matchedHashtags = words.filter(w => hashtags.contains(w.toUpperCase))
    var scoreMap = Seq[(String, (Int, Int))]()
    matchedHashtags.foreach((tag) => scoreMap = scoreMap :+(tag.toUpperCase, (score.get, 1)))
    scoreMap
  }

  val scoreSorted = scores.transform(_.sortBy(_._2._2, ascending = false))

  scoreSorted.print()

  sc.start()
  sc.awaitTermination()
}
