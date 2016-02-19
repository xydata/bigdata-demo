package org.xydata.repository.impl

import org.xydata.StatusBuilder
import org.xydata.avro.Status
import org.xydata.repository.InStream
import twitter4j._
import twitter4j.conf.Configuration

class Twitter4jStream(twitterConf: Configuration) extends InStream {

  def listen(filter: Array[String], callback: Status => Unit) = {
    val twitterStream = new TwitterStreamFactory(twitterConf).getInstance()
    val filterSPComs = new FilterQuery().track(filter: _*)
    twitterStream.addListener(new OnTweetPosted(callback))
    twitterStream.filter(filterSPComs)
  }

  class OnTweetPosted(cb: Status => Unit) extends StatusListener {

    private def toStatus(status4j: twitter4j.Status): Status = {
      StatusBuilder.build(status4j)
    }

    override def onStatus(status: twitter4j.Status): Unit = cb(toStatus(status))

    override def onException(ex: Exception): Unit = throw ex

    // no-op for the following events
    override def onStallWarning(warning: StallWarning): Unit = {}

    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}

    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}

    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}
  }


}
