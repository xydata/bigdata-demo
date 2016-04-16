package org.xydata.communication.impl

import java.util.Properties

import com.twitter.bijection.avro.SpecificAvroCodecs._
import com.typesafe.config.Config
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.xydata.avro.Status
import org.xydata.communication.MessageProducer

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
class KafkaProducer(appConf: Config) extends MessageProducer[Status] {
  lazy val kafkaBrokers = appConf.getString("kafka.brokers")
  lazy val kafkaTopic = appConf.getString("kafka.topic")
  var producer = {
    val props = new Properties()
    props.put("metadata.broker.list", kafkaBrokers)
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    new Producer[String, Array[Byte]](config)
  }

  def send(s: Status): Unit = {
    println(s.getUser + ": " + s.getText)
    val tweetEnc = toBinary[Status].apply(s)
    val msg = new KeyedMessage[String, Array[Byte]](kafkaTopic, tweetEnc)
    producer.send(msg)
  }
}
