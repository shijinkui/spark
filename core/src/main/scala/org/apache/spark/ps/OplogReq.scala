package org.apache.spark.ps

import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.spark.network.protocol.Encoders.{ByteArrays, Strings}
import org.apache.spark.ps.PValueType.ValueType

/**
 * oplog is one or more iteration's computed delta result
 */
case class OplogReq(
  iterations: Seq[Int],
  tableName: String = "0",
  key: String,
  value: Array[_],
  override val vt: ValueType
) extends PsMsgObj(PsMsg.UPD_REQ, vt)

object OplogReq extends CodecTrait[OplogReq] {

  def decode(buf: ByteBuf): OplogReq = {
    val valueType = decodeValueType(buf)
    val iterlen = buf.readInt()
    val iters = Range(0, iterlen).map(f => buf.readInt()).toSeq
    val tableName = Strings.decode(buf)
    val key = Strings.decode(buf)
    val valueBytes = ByteArrays.decode(buf)
    val value = valueType match {
      case PValueType.DOUBLE => PsBinUtil.bytes2doubles(valueBytes)
      case PValueType.LONG => PsBinUtil.bytes2longs(valueBytes)
      case PValueType.INT => PsBinUtil.bytes2ints(valueBytes)
    }

    OplogReq(iters, tableName, key, value, valueType)
  }

  override def encode(obj: OplogReq): ByteBuf = {
    val buf = Unpooled.buffer(encodedLength(obj))
    encodeType(buf, obj.mt, obj.vt)
    Strings.encode(buf, obj.tableName)
    Strings.encode(buf, obj.key)

    val vt = obj.vt match {
      case PValueType.LONG => {
        val bytes = PsBinUtil.longs2bytes(obj.value.asInstanceOf[Array[Long]])
        (bytes, PValueType.LONG)
      }
      case PValueType.DOUBLE => {
        val bytes = PsBinUtil.doubles2Bytes(obj.value.asInstanceOf[Array[Double]])
        (bytes, PValueType.DOUBLE)
      }
      case PValueType.INT => {
        val bytes = PsBinUtil.ints2bytes(obj.value.asInstanceOf[Array[Int]])
        (bytes, PValueType.INT)
      }
    }

    ByteArrays.encode(buf, vt._1)
    buf.writeByte(vt._2.id)

    buf
  }

  override def encodedLength(obj: OplogReq): Int = {
    val valueLengthUnit = obj.vt match {
      case PValueType.LONG => 8
      case PValueType.DOUBLE => 8
      case PValueType.INT => 4
    }
    val tableLen = Strings.encodedLength(obj.tableName)
    val valueLen = 4 + obj.value.length * valueLengthUnit

    //  msgType :: valueType :: iterlen :: iter1:iter2 :: tableName :: key :: valueLen :: value
    1 + 1 + 4 + obj.iterations.length * 4 + tableLen + Strings.encodedLength(obj.key) + valueLen
  }
}
