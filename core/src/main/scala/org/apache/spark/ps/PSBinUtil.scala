/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ps

/**
 * encode and decode data to bytes
 */
private[ps] object PsBinUtil {
  private val len: Int = 8

  /** double value list to bytes **/
  def doubles2Bytes(data: Array[Double]): Array[Byte] = data.map(d2b).flatten

  /** long value list to bytes **/
  def longs2bytes(data: Array[Long]): Array[Byte] = data.map(long2Bytes).flatten

  /** int value list to bytes **/
  def ints2bytes(data: Array[Int]): Array[Byte] = data.map(int2bytes).flatten

  /** bytes to double list **/
  def bytes2doubles(bytes: Array[Byte]): Array[Double] = {
    val capacity = bytes.length / len
    val ret = new Array[Double](capacity)
    var idx = 0
    while (idx < capacity) {
      val tmp = new Array[Byte](len)
      System.arraycopy(bytes, idx * len, tmp, 0, len)
      ret(idx) = b2d(tmp)
      idx = idx + 1
    }

    ret
  }

  /** bytes to long list **/
  def bytes2longs(bytes: Array[Byte]): Array[Long] = {
    val capacity = bytes.length / len
    val ret = new Array[Long](capacity)
    var idx = 0
    while (idx < capacity) {
      val tmp = new Array[Byte](len)
      System.arraycopy(bytes, idx * len, tmp, 0, len)
      ret(idx) = bytes2Long(tmp)
      idx = idx + 1
    }

    ret
  }

  /** bytes to int list **/
  def bytes2ints(bytes: Array[Byte]): Array[Int] = {
    val capacity = bytes.length / len
    val ret = new Array[Int](capacity)
    var idx = 0
    while (idx < capacity) {
      val tmp = new Array[Byte](4)
      System.arraycopy(bytes, idx * 4, tmp, 0, 4)
      ret(idx) = bytes2int(tmp)
      idx = idx + 1
    }

    ret
  }

  //  base fn


  /** double to bytes **/
  private def d2b(d: Double): Array[Byte] = {
    val l = java.lang.Double.doubleToRawLongBits(d)
    Array(
      ((l >> 56) & 0xff).toByte,
      ((l >> 48) & 0xff).toByte,
      ((l >> 40) & 0xff).toByte,
      ((l >> 32) & 0xff).toByte,
      ((l >> 24) & 0xff).toByte,
      ((l >> 16) & 0xff).toByte,
      ((l >> 8) & 0xff).toByte,
      ((l >> 0) & 0xff).toByte
    )
  }

  /** bytes to double **/
  private def b2d(bb: Array[Byte]): Double = {
    val l = ((bb(0).toLong & 0xff) << 56) |
      ((bb(1).toLong & 0xff) << 48) |
      ((bb(2).toLong & 0xff) << 40) |
      ((bb(3).toLong & 0xff) << 32) |
      ((bb(4).toLong & 0xff) << 24) |
      ((bb(5).toLong & 0xff) << 16) |
      ((bb(6).toLong & 0xff) << 8) |
      bb(7).toLong & 0xff
    java.lang.Double.longBitsToDouble(l)
  }


  /** long to bytes **/
  private def long2Bytes(x: Long): Array[Byte] = {
    Array(
      (x >> 56).toByte,
      (x >> 48).toByte,
      (x >> 40).toByte,
      (x >> 32).toByte,
      (x >> 24).toByte,
      (x >> 16).toByte,
      (x >> 8).toByte,
      x.toByte
    )
  }

  /** bytes to long **/
  private def bytes2Long(bytes: Array[Byte]): Long = {
    ((bytes(0).toLong & 0xff) << 56) |
      ((bytes(1).toLong & 0xff) << 48) |
      ((bytes(2).toLong & 0xff) << 40) |
      ((bytes(3).toLong & 0xff) << 32) |
      ((bytes(4).toLong & 0xff) << 24) |
      ((bytes(5).toLong & 0xff) << 16) |
      ((bytes(6).toLong & 0xff) << 8) |
      bytes(7).toLong & 0xff
  }


  /** int to bytes **/
  private def int2bytes(x: Int): Array[Byte] = {
    Array((x >> 24).toByte, (x >> 16).toByte, (x >> 8).toByte, x.toByte)
  }

  /** bytes to int **/
  private def bytes2int(bytes: Array[Byte]): Int = {
    ((bytes(0).toInt & 0xff) << 24) |
      ((bytes(1).toInt & 0xff) << 16) |
      ((bytes(2).toInt & 0xff) << 8) |
      bytes(3).toInt & 0xff
  }

}