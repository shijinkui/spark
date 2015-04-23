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
package org.apache.spark.network.netty

import org.apache.spark.network.client.{RpcResponseCallback, TransportClientFactory}
import org.apache.spark.network.sasl.SaslClientBootstrap
import org.apache.spark.network.server.{RpcHandler, TransportServer}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.{RpcService, TransportContext}
import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf}

import scala.collection.JavaConversions._

class NettyRpcService(conf: SparkConf) extends RpcService {

  private[this] var server: TransportServer = _
  private[this] var clientFactory: TransportClientFactory = _
  private[this] var appId: String = _

  override def init(rpcHandler: RpcHandler, cores: Option[Int],
    sm: Option[SecurityManager]): Unit = {

    val transportConf: TransportConf = SparkTransportConf.fromSparkConf(conf, cores.get)
    val bootstrap = sm.get.isAuthenticationEnabled() match {
      case false => None
      case true => Some(new SaslClientBootstrap(transportConf, conf.getAppId, sm.get))
    }
    val transportContext = new TransportContext(transportConf, rpcHandler)
    clientFactory = transportContext.createClientFactory(bootstrap.toList)
    server = transportContext.createServer(conf.getInt("spark.blockManager.port", 0))
    appId = conf.getAppId
    logInfo("Server created on " + server.getPort)
  }

  /** send payload to server */
  override def sendReqest(host: String, port: Int, payload: Array[Byte], retry: Option[Int],
    callBack: Option[RpcResponseCallback]): Unit = {
    val client = clientFactory.createClient(host, port)

    if (callBack.isDefined) {
      client.sendRpc(payload, callBack.get)
    } else {
      //  send request to server without ack to server
      client.sendRpc(payload, new RpcResponseCallback {
        override def onSuccess(response: Array[Byte]): Unit = {
          logTrace(s"Successfully send request to $host:$port, payload size:${4 + payload.length}")
        }

        override def onFailure(e: Throwable): Unit = {
          logTrace(s"Failed send request to $host:$port, payload size:${payload.length}")
        }
      })
    }
  }

  /** Tear down the transfer service. */
  override def close(): Unit = {
    server.close()
    clientFactory.close()
  }

  /** Host name the service is listening on, available only after [[init]] is invoked. */
  override def hostName: String = Utils.localHostName()

  /** Port number the service is listening on, available only after [[init]] is invoked. */
  override def port: Int = server.getPort
}
