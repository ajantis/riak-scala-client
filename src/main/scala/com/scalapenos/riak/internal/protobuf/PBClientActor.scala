/*
 * Copyright (C) 2012-2013 Age Mooij (http://scalapenos.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.scalapenos.riak
package internal
package protobuf

import akka.util.{ByteStringBuilder, ByteString}
import akka.actor.{IO => ActorIO, _}
import akka.io._

import java.net.InetSocketAddress

import scala.concurrent.duration.Duration

import com.scalapenos.riak.OperationFailed


private[riak] final class PBClientActor(serverInfo: RiakServerInfo,
                                        idleConnectionTimeout: Duration) extends Actor with ActorLogging with Stash {
  import context.system
  import Tcp._

  private val remote = new InetSocketAddress(serverInfo.host, serverInfo.pbPort)

  IO(Tcp) ! Connect(remote)

  val stages =
    new RiakPBPipelineStage >>
    new LengthFieldFrame(maxSize              = 1024 * 1024 * 50, // 50 Mb
                         lengthIncludesHeader = false) >>
    new TcpReadWriteAdapter

  val init = TcpPipelineHandler.withLogger(log, stages)

  import scala.concurrent.duration._

  context.setReceiveTimeout(idleConnectionTimeout)

  var tcpConnectionOpt: Option[ActorRef] = None

  def disconnected: Receive = {
    case Connected(_, _)           =>
      val tcpConnection = sender
      tcpConnectionOpt = Some(tcpConnection)
      val pipeline = system.actorOf(TcpPipelineHandler.props(init, tcpConnection, self).withDeploy(Deploy.local))
      context become connected(tcpConnection, pipeline)
      tcpConnection ! Register(pipeline)
      log.debug("Connected to Riak at {}:{} using protobuf api", serverInfo.host, serverInfo.pbPort)
      unstashAll()

    case CommandFailed(_: Connect) =>
      throw new ConnectionFailed(s"Failed to connect to Riak at ${serverInfo.host}:${serverInfo.pbPort} using protobuf api")

    case req: RiakPBRequest        => stash()

    case ReceiveTimeout            => // we are already disconnected
  }

  def connected(tcpConnection: ActorRef, pipeline: ActorRef): Receive = {
    case req: RiakPBRequest  =>
      context.setReceiveTimeout(Duration.Undefined)
      pipeline ! init.Command(req)
      context become processingCommand(tcpConnection, pipeline, sender)

    case _: ConnectionClosed =>
      log.error("Connection with Riak closed.")
      throw ConnectionClosed

    case ReceiveTimeout                  => disconnect(tcpConnection)
  }

  def processingCommand(tcpConnection: ActorRef, pipeline: ActorRef, recipient: ActorRef): Receive = {
    case init.Event(response: RiakPBResponse) =>
      context.setReceiveTimeout(idleConnectionTimeout)
      recipient ! response
      context become connected(tcpConnection, pipeline)
      unstashAll()

    case req: RiakPBRequest  => stash()

    case _: ConnectionClosed =>
      val t = new OperationFailed("Connection with Riak closed unexpectedly.")
      recipient ! Status.Failure(t)
      throw t
  }

  def receive = disconnected

  def disconnect(tcpConnection: ActorRef) = {
    tcpConnection ! Close
    tcpConnectionOpt = None
    context.become(disconnected)
  }

  override def postStop() {
    tcpConnectionOpt.foreach(_ ! Close)
  }
}

class RiakPBPipelineStage extends PipelineStage[PipelineContext, RiakPBRequest, ByteString, RiakPBResponse, ByteString] {
  import java.nio.ByteOrder

  def apply(ctx: PipelineContext) = new PipePair[RiakPBRequest, ByteString, RiakPBResponse, ByteString] {
    implicit val order = ByteOrder.BIG_ENDIAN

    val commandPipeline = { req: RiakPBRequest =>
      val builder = new ByteStringBuilder
      builder.putByte(PBCMsgTypes.code(req.msgType).toByte)
      builder ++= req.body
      ctx.singleCommand(builder.result())
    }

    val eventPipeline = { bs: ByteString =>
      val iter = bs.iterator
      ctx.singleEvent(RiakPBResponse(PBCMsgTypes.msgType(iter.getByte), iter.toByteString))
    }
  }
}
