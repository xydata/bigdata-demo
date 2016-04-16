package org.xydata.communication

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.xydata.communication.impl.SparkConsumer

class SparkConsumerTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfter {
  private val master = "local[2]"
  private val appName = "spark-streaming-consumer-test"
  private val batchDuration = Milliseconds(1)
  private var ssc: StreamingContext = _
  private var sparkConsumer: SparkConsumer = _

  "SparkConsumer" should "call receive method" in {
    var called = false
    intercept[IllegalArgumentException] {
      sparkConsumer.receive(_ => {
        called = true
      })
    }
    called should be(true)
  }

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(getClass.getSimpleName)
    sparkConsumer = new SparkConsumer("test-toptic", "localhost:2181", ssc)

  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
  }
}
