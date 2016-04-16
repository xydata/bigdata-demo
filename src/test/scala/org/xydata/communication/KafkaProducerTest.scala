package org.xydata.communication

import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import org.xydata.avro.Status
import org.xydata.communication.impl.KafkaProducer

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
class KafkaProducerTest extends FlatSpec with Matchers with MockitoSugar {

  val kafkaProducer: KafkaProducer = new KafkaProducer(ConfigFactory.load())
  kafkaProducer.producer = mock[Producer[String, Array[Byte]]]

  "KafkaProducer" should "work" in {
    val msg: Status = new Status()
    msg.setText("testing")
    kafkaProducer.send(msg)
  }
}
