package org.xydata.communication

import org.xydata.avro.Status

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
trait MessageProducer[M] {
  def send(s: M)
}
