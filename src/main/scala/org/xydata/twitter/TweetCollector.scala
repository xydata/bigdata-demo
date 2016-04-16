package org.xydata.twitter

object TweetCollector extends App with TweetModule {

  twitterStream.listen(hashtags, messageProducer.send)

}



