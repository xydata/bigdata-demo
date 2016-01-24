package org.xydata

import java.util.Properties

import com.twitter.bijection.avro.SpecificAvroCodecs.{toBinary, toJson}
import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.xydata.TwitterStream.OnTweetPosted
import org.xydata.avro.Tweet
import twitter4j.{FilterQuery, Status}

object KafkaProducerApp {

  private val conf = ConfigFactory.load()

  val KafkaTopic = "tweets"

  val kafkaProducer = {
    val props = new Properties()
    props.put("metadata.broker.list", conf.getString("kafka.brokers"))
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    new Producer[String, Array[Byte]](config)
  }

  val filterSPComs = new FilterQuery().track(HashtagsLoader.fetchHashtags(400): _*)


  def main(args: Array[String]) {
    val twitterStream = TwitterStream.getStream
    twitterStream.addListener(new OnTweetPosted(s => sendToKafka(toTweet(s))))
    twitterStream.filter(filterSPComs)
  }

  private def toTweet(s: Status): Tweet = {
    new Tweet(s.getUser.getName, s.getText)
  }

  private def sendToKafka(t: Tweet) {
    println(toJson(t.getSchema).apply(t))
    val tweetEnc = toBinary[Tweet].apply(t)
    val msg = new KeyedMessage[String, Array[Byte]](KafkaTopic, tweetEnc)
    kafkaProducer.send(msg)
  }

}



