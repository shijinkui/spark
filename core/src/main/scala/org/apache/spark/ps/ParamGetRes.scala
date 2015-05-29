package org.apache.spark.ps

import _root_.io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.spark.network.protocol.Encoders.ByteArrays
import org.apache.spark.ps.PValueType.{DOUBLE, INT, LONG, ValueType}

/**
 * Response of parameter getting
 * Created by sjk on 29/4/15.
 */
case class ParamGetRes(
  success: Boolean,
  payload: Option[Array[_]],
  override val vt: ValueType
) extends PsMsgObj(PsMsg.GET_RES, vt)

object ParamGetRes extends CodecTrait[ParamGetRes] {

  /** Number of bytes of the encoded form of this object. */
  override def encodedLength(obj: ParamGetRes): Int = {
    val vlen = if (obj.payload.isDefined) obj.payload.get.length * getVtLength(obj.vt) else 0
    //  msgType :: valueType :: isSuccess :: valueLength :: payload
    1 + 1 + 1 + 4 + vlen
  }

  /**
   * Serializes this object by writing into the given ByteBuf.
   * This method must write exactly encodedLength() bytes.
   */
  override def encode(obj: ParamGetRes): ByteBuf = {
    val buf = Unpooled.buffer(encodedLength(obj))
    val vlen = if (obj.payload.isDefined) obj.payload.get.length * getVtLength(obj.vt) else 0

    buf.writeByte(obj.mtValue)
    encodeType(buf, obj.mt, obj.vt)
    buf.writeBoolean(obj.success)
    buf.writeByte(obj.vt.id)
    buf.writeInt(vlen)
    //  payload
    val bytes: Array[Byte] = obj.vt match {
      case LONG => PsBinUtil.longs2bytes(obj.payload.get.asInstanceOf[Array[Long]])
      case DOUBLE => PsBinUtil.doubles2Bytes(obj.payload.get.asInstanceOf[Array[Double]])
      case INT => PsBinUtil.ints2bytes(obj.payload.get.asInstanceOf[Array[Int]])
    }
    ByteArrays.encode(buf, bytes)

    buf
  }

  override def decode(buf: ByteBuf): ParamGetRes = {
    val vtype = decodeValueType(buf)
    val success = buf.readBoolean()
    val bytes: Array[Byte] = ByteArrays.decode(buf)

    val value = vtype match {
      case LONG => PsBinUtil.bytes2longs(bytes)
      case DOUBLE => PsBinUtil.bytes2doubles(bytes)
      case INT => PsBinUtil.bytes2ints(bytes)
    }

    new ParamGetRes(success, Some(value), vtype)
  }
}
