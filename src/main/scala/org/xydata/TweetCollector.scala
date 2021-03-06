package org.xydata

import java.util.Properties

import com.twitter.bijection.avro.SpecificAvroCodecs.toBinary
import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.xydata.TwitterStream.OnTweetPosted
import org.xydata.avro.Status
import org.xydata.util.HashtagsLoader
import twitter4j.FilterQuery

object TweetCollector {

  private val conf = ConfigFactory.load()

  val KafkaTopic = "tweets"

  val kafkaProducer = {
    val props = new Properties()
    props.put("metadata.broker.list", conf.getString("kafka.brokers"))
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    new Producer[String, Array[Byte]](config)
  }

  def main(args: Array[String]) {
    val twitterStream = TwitterStream.getStream
    val filterSPComs = new FilterQuery().track(HashtagsLoader.fetchHashtags(): _*)
    twitterStream.addListener(new OnTweetPosted(s => sendToKafka(toStatus(s))))
    twitterStream.filter(filterSPComs)
  }

  private def toStatus(status4j: twitter4j.Status): Status = {
    StatusBuilder.build(status4j)
  }

  private def sendToKafka(s: Status) {
    println(s.getUser + ": " + s.getText)
    val tweetEnc = toBinary[Status].apply(s)
    val msg = new KeyedMessage[String, Array[Byte]](KafkaTopic, tweetEnc)
    kafkaProducer.send(msg)
  }

}



