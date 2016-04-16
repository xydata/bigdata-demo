package org.xydata.repository

import com.typesafe.config.ConfigFactory
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import org.xydata.repository.impl.Twitter4jStream
import twitter4j.conf.{Configuration, ConfigurationBuilder}

class Twitter4jStreamTest extends FlatSpec with Matchers with MockitoSugar {
  lazy val appConf = ConfigFactory.load()
  lazy val twitterConf: Configuration = {
    new ConfigurationBuilder()
      .setOAuthConsumerKey(appConf.getString("twitter.consumerKey"))
      .setOAuthConsumerSecret(appConf.getString("twitter.consumerSecret"))
      .setOAuthAccessToken(appConf.getString("twitter.accessToken"))
      .setOAuthAccessTokenSecret(appConf.getString("twitter.accessTokenSecret"))
      .build()
  }
  "Twitter4jStream" should "listen with callback" in {
    val twitter4jStream = new Twitter4jStream(twitterConf)
    var called = false
    twitter4jStream.listen(Array(), _ => {
      called = true
    })
    // no twitter is received
    called should be(false)
  }
}
