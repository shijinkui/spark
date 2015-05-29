package org.apache.spark.ps

import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.spark.network.protocol.Encoders.Strings
import org.apache.spark.ps.PValueType.ValueType

/**
 * [[PsMsg.GET_REQ]] type request
 */
case class ParamGetReq(
  tableName: String = "w",
  key: String,
  range: Option[Range],
  seq: Option[Array[Int]],
  override val vt: ValueType) extends PsMsgObj(PsMsg.GET_REQ, vt)

object ParamGetReq extends CodecTrait[ParamGetReq] {

  override def encodedLength(obj: ParamGetReq): Int = {

    val indexLength = if (obj.range.isDefined) {
      8
    } else if (obj.seq.isDefined) {
      4 + obj.seq.get.length * 4
    } else {
      0
    }

    1 + 1 + Strings.encodedLength(obj.tableName) + Strings.encodedLength(obj.key) + 1 + indexLength
  }

  override def encode(obj: ParamGetReq): ByteBuf = {
    val buf = Unpooled.buffer(encodedLength(obj))
    encodeType(buf, obj.mt, obj.vt)
    Strings.encode(buf, obj.tableName)
    Strings.encode(buf, obj.key)

    if (obj.range.isDefined) {
      buf.writeByte(IndexType.RANGE.id)
      buf.writeInt(obj.range.get.start)
      buf.writeInt(obj.range.get.end)
    } else if (obj.seq.isDefined) {
      buf.writeByte(IndexType.SEQ.id)
      buf.writeInt(obj.seq.get.length)
      obj.seq.get.foreach(buf.writeInt)
    } else {
      buf.writeByte(IndexType.NO.id)
    }

    buf
  }

  override def decode(buf: ByteBuf): ParamGetReq = {
    //  decode type out this obj
    val vt = decodeValueType(buf)
    val table = Strings.decode(buf)
    val key = Strings.decode(buf)
    var range: Option[Range] = None
    var seq: Option[Array[Int]] = None

    val indexType = buf.readByte
    IndexType(indexType) match {
      case IndexType.RANGE => range = Some(Range(buf.readInt(), buf.readInt()))
      case IndexType.SEQ =>
        val len = buf.readInt()
        seq = Some((0 until len).map(f => buf.readInt()).toArray)
      case _ =>
    }

    ParamGetReq(table, key, range, seq, vt)
  }
}
