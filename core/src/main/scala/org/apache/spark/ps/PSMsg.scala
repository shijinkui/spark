package org.apache.spark.ps

import org.apache.spark.ps.PValueType._
import org.apache.spark.ps.PsMsg.MsgType

/**
 * parameter message type
 * Created by sjk on 27/4/15.
 */

class PsMsgObj(msgType: MsgType, valueType: ValueType) {
  def mt: MsgType = msgType

  def mtValue: Int = mt.id

  def vt: ValueType = valueType
}

object PsMsg extends Enumeration {
  type MsgType = Value

  //  get request from client
  val GET_REQ = Value(10)

  //  get response from server
  val GET_RES = Value(11)

  //  update request from client
  val UPD_REQ = Value(12)

  //  update response from server
  val UPD_RES = Value(13)
}


object IndexType extends Enumeration {
  type IndexType = Value
  val NO, RANGE, SEQ = Value
}