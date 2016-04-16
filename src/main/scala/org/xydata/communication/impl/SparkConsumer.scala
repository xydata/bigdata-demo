package org.xydata.communication.impl

import com.twitter.bijection.avro.SpecificAvroCodecs
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.xydata.avro.Status
import org.xydata.communication.MessageConsumer

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
class SparkConsumer(kafkaTopic: String, zookeeperQuorum: String, streamingContext: StreamingContext) extends MessageConsumer[DStream[Status]] {


  override def receive(analyze: DStream[Status] => Unit): Unit = {

    val encTweets = {
      val topics = Map(kafkaTopic -> 1)
      val kafkaParams = Map(
        "zookeeper.connect" -> zookeeperQuorum,
        "group.id" -> "1")
      KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
        streamingContext, kafkaParams, topics, StorageLevel.MEMORY_ONLY)
    }

    val tweets = encTweets.flatMap(x => SpecificAvroCodecs.toBinary[Status].invert(x._2).toOption)

    analyze(tweets)

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
