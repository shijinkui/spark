package org.apache.spark.ps

import io.netty.channel.socket.SocketChannel
import io.netty.channel.{Channel, ChannelPipeline}
import io.netty.handler.timeout.IdleStateHandler
import org.apache.spark.Logging
import org.apache.spark.network.client.{TransportClient, TransportResponseHandler}
import org.apache.spark.network.protocol.{MessageDecoder, MessageEncoder}
import org.apache.spark.network.server.{TransportChannelHandler, TransportRequestHandler}
import org.apache.spark.network.util.{NettyUtils, TransportConf}

/**
 * base netty service
 */
private[spark] trait BaseNettyService extends Logging {

  def initializePipeline(channel: SocketChannel, conf: TransportConf) = {
    try {
      val channelHandler: TransportChannelHandler = createChannelHandler(channel, conf)
      val pipeline: ChannelPipeline = channel.pipeline
      pipeline.addLast("encoder", new MessageEncoder)
        .addLast("frameDecoder", NettyUtils.createFrameDecoder)
        .addLast("decoder", new MessageDecoder())
        .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs / 1000))
        .addLast("handler", channelHandler)
      channelHandler
    }
    catch {
      case e: RuntimeException => {
        logError("Error while initializing Netty pipeline", e)
        throw e
      }
    }
  }

  /**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   */
  private def createChannelHandler(ch: Channel, conf: TransportConf): TransportChannelHandler = {

    val responseHandler = new TransportResponseHandler(ch)
    val client = new TransportClient(ch, responseHandler)
    val requestHandler = new TransportRequestHandler(ch, client, rpcHandler)
    new TransportChannelHandler(client, responseHandler, requestHandler, conf.connectionTimeoutMs)
  }

}
