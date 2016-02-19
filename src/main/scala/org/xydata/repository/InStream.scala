package org.xydata.repository

import org.xydata.avro.Status

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
trait InStream {
  def listen(filter: Array[String], callback: Status => Unit)
}
