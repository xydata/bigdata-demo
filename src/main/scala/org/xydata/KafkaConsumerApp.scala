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

  val tweets = encTweets.flatMap(x => SpecificAvroCodecs.toBinary[Tweet].invert(x._2).toOption)
  // TODO add filter and calculation code
  val wordCounts = tweets.flatMap(_.getText.split(" ")).map((_, 1)).reduceByKey(_ + _)
  val countsSorted = wordCounts.transform(_.sortBy(_._2, ascending = false))

  countsSorted.print()

  sc.start()
  sc.awaitTermination()
}
