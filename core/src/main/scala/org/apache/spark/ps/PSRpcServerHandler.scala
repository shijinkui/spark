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
import org.apache.spark.Logging
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}

/**
 * serve process parameter request
 */
class PSRpcServerHandler(storage: PStorage) extends RpcHandler with Logging {

  private lazy val streamManager = new OneForOneStreamManager()

  override def receive(
    client: TransportClient,
    messageBytes: Array[Byte],
    callback: RpcResponseCallback): Unit = {

    val buf: ByteBuf = Unpooled.wrappedBuffer(messageBytes)
    val tpe = buf.readByte

    //  ParameterTask: TrainingTask -> ParameterTask, decode request
    PsMsg(tpe) match {
      case PsMsg.GET_REQ =>
        val req = ParamGetReq.decode(buf)
        //  get value from storage
        val values = storage.get(req.tableName, req.key)
        //  build and send response to client
        val ret = new ParamGetRes(success = true, Some(values), storage.vt)
        callback.onSuccess(ParamGetRes.encode(ret).array())
      case PsMsg.UPD_REQ =>
        val req = OplogReq.decode(buf)
        storage.update(req.tableName, req.key, req.value)
    }
  }

  override def getStreamManager: StreamManager = streamManager
}
