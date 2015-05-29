package org.apache.spark.ps

import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.spark.ps.PValueType.ValueType

/**
 * response of receive oplog request
 * @param success whether success for sending request to ParameterServer
 * @param curIteration current iteration of ParameterServer
 */
case class OplogRes(
  success: Boolean,
  curIteration: Int,
  override val vt: ValueType) extends PsMsgObj(PsMsg.UPD_RES, vt)

object OplogRes extends CodecTrait[OplogRes] {

  override def encodedLength(obj: OplogRes): Int = {
    //  msgType :: valueType :: success :: curIteration
    1 + 1 + 1 + 4
  }

  override def encode(obj: OplogRes): ByteBuf = {
    val buf = Unpooled.buffer(encodedLength(obj))
    encodeType(buf, obj.mt, obj.vt)
    buf.writeBoolean(obj.success)
    buf.writeInt(obj.curIteration)

    buf
  }

  override def decode(buf: ByteBuf): OplogRes = {
    val vt = decodeValueType(buf)
    val success = buf.readBoolean()
    val curIter = buf.readInt()
    OplogRes(success, curIter, vt)
  }
}
