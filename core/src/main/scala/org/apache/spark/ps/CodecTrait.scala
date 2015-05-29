package org.apache.spark.ps

import io.netty.buffer.ByteBuf
import org.apache.spark.ps.PValueType._
import org.apache.spark.ps.PsMsg.MsgType

/**
 * decode interface
 */
trait CodecTrait[T] {

  def encodedLength(obj: T): Int

  def decode(buf: ByteBuf): T

  def encode(obj: T): ByteBuf

  def getVtLength(tp: ValueType): Int = {
    import org.apache.spark.ps.PValueType._
    tp match {
      case LONG => 8
      case DOUBLE => 8
      case INT => 4
    }
  }

  def decodeValueType(buf: ByteBuf): ValueType = {
    val tpe = buf.readByte()
    PValueType(tpe)
  }

  def encodeType(buf: ByteBuf, mt: MsgType, vt: ValueType): Unit = {
    buf.writeByte(mt.id)
    buf.writeByte(vt.id)
  }
}
