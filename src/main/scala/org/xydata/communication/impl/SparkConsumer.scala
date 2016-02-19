package org.xydata.communication.impl

import com.twitter.bijection.avro.SpecificAvroCodecs
import com.typesafe.config.Config
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.xydata.avro.Status
import org.xydata.communication.MessageConsumer

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
class SparkConsumer(appConf: Config) extends MessageConsumer[DStream[Status]] {

  lazy val kafkaBrokers = appConf.getString("kafka.brokers")
  lazy val kafkaTopic = appConf.getString("kafka.topic")
  lazy val sparkMaster = appConf.getString("spark.master.url")
  lazy val tweeterHeartbeat = appConf.getInt("tweeter.stream.heartbeat")
  lazy val zookeeperQuorum = appConf.getString("kafka.zookeeper.quorum")

  override def receive(analyze: DStream[Status] => Unit): Unit = {

    val sparkConf = new SparkConf().setAppName("bigdata-demo").setMaster(sparkMaster)
    val sc = new StreamingContext(sparkConf, Seconds(tweeterHeartbeat))

    val encTweets = {
      val topics = Map(kafkaTopic -> 1)
      val kafkaParams = Map(
        "zookeeper.connect" -> zookeeperQuorum,
        "group.id" -> "1")
      KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        sc, kafkaParams, topics, StorageLevel.MEMORY_ONLY)
    }

    val tweets = encTweets.flatMap(x => SpecificAvroCodecs.toBinary[Status].invert(x._2).toOption)

    analyze(tweets)

    sc.start()
    sc.awaitTermination()
  }

}
