package org.xydata.twitter

import com.softwaremill.macwire._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.xydata.analysis.Analyzer
import org.xydata.analysis.impl.TwitterSparkAnalyzer
import org.xydata.avro.Status
import org.xydata.communication.impl.{KafkaProducer, SparkConsumer}
import org.xydata.communication.{MessageConsumer, MessageProducer}
import org.xydata.repository.impl._
import org.xydata.repository.{DictionaryDao, HashtagsDao, InStream}
import twitter4j.conf.{Configuration, ConfigurationBuilder}

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
@Module
trait TweetModule {

  // configuration
  lazy val appConf = ConfigFactory.load()
  lazy val twitterConf: Configuration = {
    new ConfigurationBuilder()
      .setOAuthConsumerKey(appConf.getString("twitter.consumerKey"))
      .setOAuthConsumerSecret(appConf.getString("twitter.consumerSecret"))
      .setOAuthAccessToken(appConf.getString("twitter.accessToken"))
      .setOAuthAccessTokenSecret(appConf.getString("twitter.accessTokenSecret"))
      .build()
  }
  lazy val sparkMaster = appConf.getString("spark.master.url")
  lazy val tweeterHeartbeat = appConf.getInt("tweeter.stream.heartbeat")
  lazy val sparkConf = new SparkConf().setAppName("bigdata-demo").setMaster(sparkMaster)
  lazy val streamingContext = new StreamingContext(sparkConf, Milliseconds(tweeterHeartbeat))
  lazy val kafkaTopic = appConf.getString("kafka.topic")
  lazy val zookeeperQuorum = appConf.getString("kafka.zookeeper.quorum")

  // beans wiring
  lazy val twitterAnalyzer: Analyzer[DStream[Status]] = new TwitterSparkAnalyzer(appConf.getInt("tweeter.stream.window"), dictionary, hashtags)
  lazy val messageProducer: MessageProducer[Status] = wire[KafkaProducer]
  lazy val messageConsumer: MessageConsumer[DStream[Status]] = new SparkConsumer(kafkaTopic, zookeeperQuorum, streamingContext)
  lazy val dictionaryDao: DictionaryDao = wire[DictionaryCSVLoader]
  lazy val hashtagsDao: HashtagsDao = wire[HashtagsCSVLoader]
  lazy val twitterStream: InStream = wire[Twitter4jStream]
  lazy val dictionary: Map[String, Int] = dictionaryDao.fetch()
  lazy val hashtags: Array[String] = hashtagsDao.fetch()
}
