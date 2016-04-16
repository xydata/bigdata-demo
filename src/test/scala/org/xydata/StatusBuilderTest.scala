package org.xydata

import java.util.Date

import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import twitter4j.Status

class StatusBuilderTest extends FlatSpec with MockitoSugar {

  "StatusBuilder" should "build status from twitter object" in {
    val status: Status = mock[Status]
    when(status.getCreatedAt).thenReturn(new Date())
    val builded = StatusBuilder.build(status)
    builded.getCreatedAt should not be null
  }

}
