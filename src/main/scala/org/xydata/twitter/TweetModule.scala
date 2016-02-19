package org.xydata.twitter

import com.softwaremill.macwire._
import com.typesafe.config.ConfigFactory
import org.xydata.analysis.impl.TwitterSparkAnalyzer
import org.xydata.avro.Status
import org.xydata.communication.MessageProducer
import org.xydata.communication.impl.{KafkaProducer, SparkConsumer}
import org.xydata.repository.impl._
import org.xydata.repository.{DictionaryDao, HashtagsDao, InStream}
import twitter4j.conf.{Configuration, ConfigurationBuilder}

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
@Module
trait TweetModule {

  // configuration
  lazy val conf = ConfigFactory.load()
  lazy val twitterConf: Configuration = {
    new ConfigurationBuilder()
      .setOAuthConsumerKey(conf.getString("twitter.consumerKey"))
      .setOAuthConsumerSecret(conf.getString("twitter.consumerSecret"))
      .setOAuthAccessToken(conf.getString("twitter.accessToken"))
      .setOAuthAccessTokenSecret(conf.getString("twitter.accessTokenSecret"))
      .build()
  }

  // beans wiring
  lazy val twitterAnalyzer = wire[TwitterSparkAnalyzer]
  lazy val messageProducer: MessageProducer[Status] = wire[KafkaProducer]
  lazy val messageConsumer = wire[SparkConsumer]
  lazy val dictionaryDao: DictionaryDao = wire[DictionaryCSVLoader]
  lazy val hashtagsDao: HashtagsDao = wire[HashtagsCSVLoader]
  lazy val twitterStream: InStream = wire[Twitter4jStream]
  lazy val dictionary = dictionaryDao.fetch()
  lazy val hashtags = hashtagsDao.fetch()
}
