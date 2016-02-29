package org.xydata.communication

import org.scalatest.{FlatSpec, Matchers}
import org.xydata.avro.{Status, User}
import org.xydata.mining.twitter.TweetModule

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
class KafkaProducerTest extends FlatSpec with Matchers with TweetModule {
  "KafkaProducer" should "work" in {
    val msg: Status = new Status()
    msg.setText("testing")
    messageProducer.send(msg)
  }
}
