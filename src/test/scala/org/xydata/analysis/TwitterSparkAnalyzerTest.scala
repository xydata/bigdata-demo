package org.xydata.analysis

import java.io.File
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.scalatest.{FlatSpec, Matchers}
import org.xydata.avro.{Status, User}
import org.xydata.mining.twitter.TweetModule

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
class TwitterSparkAnalyzerTest extends FlatSpec with Matchers with TweetModule {

  "TwitterSparkAnalyzer" should "score simple phrase" in {
    twitterAnalyzer.scoring("$AAPL is good") should equal(Seq() :+("$AAPL", (1, 1)))
  }

  "TwitterSparkAnalyzer" should "score 2 stocks" in {
    twitterAnalyzer.scoring("$ADBE is good $AAPL is good") should equal(Seq() :+("$ADBE", (2, 1)) :+("$AAPL", (2, 1)))
  }

  "TwitterSparkAnalyzer" should "recognize negative influence" in {
    twitterAnalyzer.scoring("$AAPL is bad") should equal(Seq() :+("$AAPL", (-1, 1)))
  }

  "TwitterSparkAnalyzer" should "summarize when reducing" in {
    twitterAnalyzer.reduceScore((1, 2), (3, 4)) should equal((4, 6))
  }

  "TwitterSparkAnalyzer" should "be able to analyze spark stream" in {
    val ssc = initSparkStreamContext
    val dir = initDir
    def mapToStatus(s: String): Status = {
      val status = new Status()
      val u: User = new User()
      u.setName("test")
      status.setUser(u)
      status.setText(s)
      status
    }
    twitterAnalyzer.analyze(ssc.textFileStream(dir).map(mapToStatus))
  }

  def initDir: String = {
    val tmp = Files.createTempDirectory(getClass.getSimpleName)
    val tmpFile = File.createTempFile("testing", ".tmp", tmp.toFile)
    tmpFile.deleteOnExit()
    scala.tools.nsc.io.File(tmpFile.getPath).writeAll("$APPL good")
    tmp.toAbsolutePath.toString
  }

  def initSparkStreamContext: StreamingContext = {
    val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster(appConf.getString("spark.master.url"))
    new StreamingContext(conf, Milliseconds(appConf.getInt("tweeter.stream.heartbeat")))
  }

}