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
package internal.protobuf

import akka.io._
import akka.actor._
import akka.util.{Timeout, ByteStringBuilder, ByteString}
import akka.pattern.ask

import java.net.InetSocketAddress
import akka.io.IO
import scala.concurrent.Future

import com.basho.riak.protobuf._

import serialization.PBConversions._
import com.scalapenos.riak.internal.DateTimeSupport._
import scala.concurrent.duration._
import com.scalapenos.riak.ETag
import com.scalapenos.riak.BucketOperationFailed
import scala.util.Failure
import com.scalapenos.riak.VClock
import com.scalapenos.riak.ConflictResolution
import scala.util.Success

private[riak] final class RiakPBClientHelper(actorSystem: ActorSystem) {
  implicit val system = actorSystem

  def clientActor: ActorRef = system.actorOf(Props[PBClientActor])

  import system.dispatcher


  implicit val timeout = Timeout(6.seconds)

  def ping(): Future[Boolean] = {
    val response: Future[RiakPBResponse] = (clientActor ? RiakPBRequest(PBCMsgTypes.RpbPingReq)).mapTo[RiakPBResponse]
    response.map(_.msgType == PBCMsgTypes.RpbPingResp)
  }

  def fetch(bucket: String, key: String, resolver: RiakConflictsResolver): Future[Option[RiakValue]] = {
    val response: Future[RiakPBResponse] =
      (clientActor ? RiakPBRequest(PBCMsgTypes.RpbGetReq, RpbGetReq(bucket = bucket, key = key))).mapTo[RiakPBResponse]

    response.map { r =>
      val tryR = responseToTry(r)
      tryR match {
        case Success(r) =>
          val riakResponse = RpbGetResp().mergeFrom(r.body.toArray)
          riakResponse.`content`.toList match {
            case Nil => None

            case x :: Nil => Some(toRiakValue(x))

            case l @ (x :: xs) =>
              val ConflictResolution(resolvedValue, _) = resolver.resolve(l.map(toRiakValue(_)).toSet)
              Some(resolvedValue)
          }

        case Failure(f) => throw new BucketOperationFailed(s"Fetch for key '$key' in bucket '$bucket' produced an unexpected response error '$f'.")
      }
    }
  }

  def storeAndFetch(bucket: String, key: String, value: RiakValue, resolver: RiakConflictsResolver): Future[RiakValue] = {
    val response: Future[RiakPBResponse] =
      (clientActor ? RiakPBRequest(PBCMsgTypes.RpbPutReq,
        RpbPutReq(bucket = bucket, key = Some(key), `returnBody` = Some(true), content = toRbpContent(value)))).mapTo[RiakPBResponse]

    response.map { r =>
      val tryR = responseToTry(r)
      tryR match {
        case Success(r) =>
          val riakResponse = RpbPutResp().mergeFrom(r.body.toArray)
          riakResponse.`content`.toList match {
            case Nil => throw new Exception("Write is not successful")

            case x :: Nil => toRiakValue(x)

            case l @ (x :: xs) =>
              val ConflictResolution(resolvedValue, _) = resolver.resolve(l.map(toRiakValue(_)).toSet)
              resolvedValue
          }

        case Failure(f) => throw new BucketOperationFailed(s"Fetch for key '$key' in bucket '$bucket' produced an unexpected response error '$f'.")
      }
    }
  }

  def delete(bucket: String, key: String): Future[Unit] = {
    val response: Future[RiakPBResponse] =
      (clientActor ? RiakPBRequest(PBCMsgTypes.RpbDelReq, RpbDelReq(bucket = bucket, key = key))).mapTo[RiakPBResponse]

    response.map { r =>
      val tryR = responseToTry(r)
      tryR match {
        case Success(r) =>
          require(r.msgType == PBCMsgTypes.RpbDelResp)

        case Failure(f) => throw new BucketOperationFailed(s"Fetch for key '$key' in bucket '$bucket' produced an unexpected response error '$f'.")
      }
    }
  }

  def toRiakValue(content: RpbContent): RiakValue = {
    RiakValue(content.`value`.toStringUtf8,
      //content.`contentType`, // TDODO content type
      ContentTypes.NoContentType,
      content.`vtag`.map(VClock(_)).getOrElse(VClock.NotSpecified),
      ETag.NotSpecified,
      currentDateTimeUTC)
  }

  def toRbpContent(riakValue: RiakValue): RpbContent =
    new RpbContent().copy(
      `value` = riakValue.data,
      `vtag` = Some(riakValue.vclock.value),
      `lastMod` = Some(riakValue.lastModified.getMillis.toInt)) // TODO
}

private[riak] final class PBClientActor extends Actor with ActorLogging with Stash {
  import Tcp._
  import context.system

  implicit val timeout = Timeout(3.second)

  val remote = new InetSocketAddress("127.0.0.1", 8087)
  IO(Tcp) ! Connect(remote)

  val stages = new RiakPBPipelineStage >>
    new LengthFieldFrame(maxSize = 1024 * 1024 * 50, lengthIncludesHeader = false) >>
    new TcpReadWriteAdapter
  val init = TcpPipelineHandler.withLogger(log, stages)

  var recipient: ActorRef = _

  def receive = {
    case Connected(_, _) =>
      val connection = sender
      val pipeline = system.actorOf(TcpPipelineHandler.props(init, connection, self).withDeploy(Deploy.local))
      context become connected(pipeline)
      connection ! Register(pipeline)
      unstashAll()

    case CommandFailed(_: Connect) =>
      log.error("Failed to connect to host.")
      context stop self

    case r: RiakPBRequest => stash()
  }

  def connected(pipeline: ActorRef): Receive = {
    case req: RiakPBRequest  =>
      recipient = sender
      pipeline ! init.Command(req)

    case init.Event(r: RiakPBResponse) =>
      recipient ! r
      context stop self

    case _: ConnectionClosed =>
      println("Connection is closed.")
      context stop self
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

sealed trait RiakPBMessage {
  def msgType: PBCMsgType
  def body: ByteString
}

case class RiakPBRequest(msgType: PBCMsgType, body: ByteString = ByteString()) extends RiakPBMessage
case class RiakPBResponse(msgType: PBCMsgType, body: ByteString = ByteString()) extends RiakPBMessage

sealed trait PBCMsgType

object PBCMsgTypes {
  case object RpbErrorResp extends PBCMsgType // 0
  case object RpbPingReq extends PBCMsgType // 1
  case object RpbPingResp extends PBCMsgType // 2
  case object RpbGetClientIdReq extends PBCMsgType // 3
  case object RpbGetClientIdResp extends PBCMsgType // 4
  case object RpbSetClientIdReq extends PBCMsgType // 5
  case object RpbSetClientIdResp extends PBCMsgType // 6
  case object RpbGetServerInfoReq extends PBCMsgType // 7
  case object RpbGetServerInfoResp extends PBCMsgType // 8
  case object RpbGetReq extends PBCMsgType // 9
  case object RpbGetResp extends PBCMsgType // 10
  case object RpbPutReq extends PBCMsgType // 11
  case object RpbPutResp extends PBCMsgType // 12
  case object RpbDelReq extends PBCMsgType // 13
  case object RpbDelResp extends PBCMsgType // 14
  case object RpbListBucketsReq extends PBCMsgType // 15
  case object RpbListBucketsResp extends PBCMsgType // 16
  case object RpbListKeysReq extends PBCMsgType // 17
  case object RpbListKeysResp extends PBCMsgType // 18
  case object RpbGetBucketReq extends PBCMsgType // 19
  case object RpbGetBucketResp extends PBCMsgType // 20
  case object RpbSetBucketReq extends PBCMsgType // 21
  case object RpbSetBucketResp extends PBCMsgType // 22
  case object RpbMapRedReq extends PBCMsgType // 23
  case object RpbMapRedResp extends PBCMsgType // 24
  case object RpbIndexReq extends PBCMsgType // 25
  case object RpbIndexResp extends PBCMsgType // 26
  case object RpbSearchQueryReq extends PBCMsgType // 27
  case object RbpSearchQueryResp extends PBCMsgType // 28

  val values = Map[PBCMsgType, Int](
    RpbErrorResp -> 0,
    RpbPingReq -> 1,
    RpbPingResp -> 2,
    RpbGetClientIdReq -> 3,
    RpbGetClientIdResp -> 4,
    RpbSetClientIdReq -> 5,
    RpbSetClientIdResp -> 6,
    RpbGetServerInfoReq -> 7,
    RpbGetServerInfoResp -> 8,
    RpbGetReq -> 9,
    RpbGetResp -> 10,
    RpbPutReq -> 11,
    RpbPutResp -> 12,
    RpbDelReq -> 13,
    RpbDelResp -> 14,
    RpbListBucketsReq -> 15,
    RpbListBucketsResp -> 16,
    RpbListKeysReq -> 17,
    RpbListKeysResp -> 18,
    RpbGetBucketReq -> 19,
    RpbGetBucketResp -> 20,
    RpbSetBucketReq -> 21,
    RpbSetBucketResp -> 22,
    RpbMapRedReq -> 23,
    RpbMapRedResp -> 24,
    RpbIndexReq -> 25,
    RpbIndexResp -> 26,
    RpbSearchQueryReq -> 27,
    RbpSearchQueryResp -> 28)

  def code(t: PBCMsgType): Int = values(t)

  def msgType(code: Int): PBCMsgType = values.toList.find(_._2 == code).map(_._1).getOrElse(throw new IllegalArgumentException("PDC code not found"))
}