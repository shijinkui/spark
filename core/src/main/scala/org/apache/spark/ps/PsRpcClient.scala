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

import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient, TransportClientFactory}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.{Logging, SparkConf}

/**
 * start a netty server for listen request of update/get parameter
 */
private[spark] class PsRpcClient(
  conf: SparkConf,
  numCores: Int,
  callBack: PStorage) extends Logging {

  private[this] var clientFactory: TransportClientFactory = _

  def init(): Unit = {
    val transportConf = SparkTransportConf.fromSparkConf(conf, numCores)
    val rpcHandler = new PSRpcServerHandler(callBack)
    val transportContext = new TransportContext(transportConf, rpcHandler)
    clientFactory = transportContext.createClientFactory()
  }

  def sendRequest(remoteHost: String, remotePort: Int, msg: PsMsgObj, callback: PCallback) = {

    val bytes: Array[Byte] = msg match {
      case get: ParamGetReq => ParamGetReq.encode(get).array()
      case up: OplogReq => OplogReq.encode(up).array()
    }

    val client: TransportClient = clientFactory.createClient(remoteHost, remotePort)
    client.sendRpc(bytes, new RpcResponseCallback() {
      override def onSuccess(messageBytes: Array[Byte]): Unit = {
        //  client callback
        val buf: ByteBuf = Unpooled.wrappedBuffer(messageBytes)
        val tpe = buf.readByte
        val msg = PsMsg(tpe) match {
          case PsMsg.GET_RES => ParamGetRes.decode(buf)
          case PsMsg.UPD_RES => OplogRes.decode(buf)
        }

        callback.onSuccess(msg)
      }

      override def onFailure(e: Throwable): Unit = callback.onFailure(e)
    })
  }

  def close(): Unit = {
    clientFactory.close()
  }
}
