package org.xydata.analysis

import org.scalatest.{FlatSpec, Matchers}
import org.xydata.twitter.TweetModule

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
    twitterAnalyzer.reduce((1, 2), (3, 4)) should equal((4, 6))
  }
}