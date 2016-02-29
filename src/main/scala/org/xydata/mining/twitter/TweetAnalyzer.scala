package org.xydata.mining.twitter

object TweetAnalyzer extends App with TweetModule {

  messageConsumer.receive(twitterAnalyzer.analyze)

}
