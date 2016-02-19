package org.xydata.communication

/**
  * Created by "Yunbo WANG" on 19/02/16.
  */
trait MessageConsumer[M] {
  def receive(analyze: M => Unit)
}
