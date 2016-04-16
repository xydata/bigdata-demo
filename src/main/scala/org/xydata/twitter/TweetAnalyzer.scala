package org.xydata.twitter

object TweetAnalyzer extends App with TweetModule {

  messageConsumer.receive(twitterAnalyzer.analyze)

}
