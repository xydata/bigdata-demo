package org.xydata.analysis

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.xydata.analysis.impl.TwitterSparkAnalyzer
import org.xydata.avro.{Status, User}

import scala.collection.mutable

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
class TwitterSparkAnalyzerTest extends FlatSpec with Matchers with BeforeAndAfter with Eventually {

  private val master = "local[2]"
  private val appName = "spark-streaming-test"
  private val batchDuration = Milliseconds(1)
  private var sc: SparkContext = _
  private var ssc: StreamingContext = _
  private var twitterAnalyzer: TwitterSparkAnalyzer = _

  "TwitterSparkAnalyzer" should "score simple phrase" in {
    twitterAnalyzer.scoring("$AAPL is good") should equal(Seq(("$AAPL", (1, 1))))
  }

  "TwitterSparkAnalyzer" should "score 2 stocks" in {
    twitterAnalyzer.scoring("$ADBE is good $AAPL is good") should equal(Seq(("$ADBE", (2, 1)), ("$AAPL", (2, 1))))
  }

  "TwitterSparkAnalyzer" should "recognize negative influence" in {
    twitterAnalyzer.scoring("$AAPL is bad") should equal(Seq(("$AAPL", (-1, 1))))
  }

  "TwitterSparkAnalyzer" should "summarize when reducing" in {
    twitterAnalyzer.reduceScore((1, 2), (3, 4)) should equal((4, 6))
  }

  "TwitterSparkAnalyzer" should "be able to analyze spark stream" in {
    def mapToStatus(s: String): Status = {
      val status = new Status()
      val u: User = new User()
      u.setName("test")
      status.setUser(u)
      status.setText(s)
      status
    }
    val queue = mutable.Queue[RDD[String]]()
    queue += sc.makeRDD(Seq("$APPL is good", "$ADBE is bad"))
    val dstream = ssc.queueStream(queue)
    twitterAnalyzer.analyze(dstream.map(mapToStatus))
    ssc.start()
  }

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    ssc = new StreamingContext(conf, batchDuration)
    sc = ssc.sparkContext
    ssc.checkpoint("build/" + getClass.getSimpleName)

    twitterAnalyzer = new TwitterSparkAnalyzer(
      1,
      Map("good" -> 1, "bad" -> -1),
      Array("$AAPL", "$ADBE")
    )
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
  }
}