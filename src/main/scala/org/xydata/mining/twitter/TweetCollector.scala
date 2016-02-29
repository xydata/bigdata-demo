package org.xydata.mining.twitter

object TweetCollector extends App with TweetModule {

  twitterStream.listen(hashtags, messageProducer.send)

}



