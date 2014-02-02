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

import akka.actor._
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.Future
import scala.util.{Left, Right}

import serialization.PBConversions._
import org.parboiled.common.Base64

import com.basho.riak.protobuf._
import com.google.protobuf.{ByteString => PBByteString}
import akka.routing.{DefaultResizer, RoundRobinRouter}
import akka.actor.SupervisorStrategy.{Resume, Restart}

import scala.concurrent.duration._


private[riak] final class RiakPBClientHelper(system: ActorSystem, server: RiakServerInfo) {
  import system.dispatcher

  private val settings = RiakClientExtension(system).settings

  implicit val timeout = Timeout(settings.DefaultFutureTimeout)

  // Used to encode/decode Riak VClocks
  private val base64 = Base64.rfc2045()

  // Default values for Riak
  private val DefaultNumberOfReplicas = 3
  private val DefaultLastWriteWins    = false
  private val DefaultAllowMult        = false

  // Type alias for convenience: we get from Riak PB interface either String message with error or a response object
  private type RiakReply = Either[String, RiakPBResponse]

  val clientSupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = settings.MaxNumberOfProtobufRetries, withinTimeRange = 2.seconds) {
      case _ => Restart
    }

  private val resizer = DefaultResizer(
      lowerBound        = settings.MinNumberProtobufConnections,
      upperBound        = settings.MaxNumberProtobufConnections,
      messagesPerResize = settings.ProtobufQueueBeforeResizingPool)

  private val clientActor: ActorRef =
    system.actorOf(
      Props(classOf[PBClientActor], server, settings.ProtobufConnectionIdleTimeout)
        .withRouter(RoundRobinRouter(supervisorStrategy = clientSupervisorStrategy, resizer = Some(resizer))))

  def ping: Future[Boolean] =
    sendRequest(RiakPBRequest(PBCMsgTypes.RpbPingReq)).map {
      case Right(r) if r.msgType == PBCMsgTypes.RpbPingResp => true
      case _                                                => false
    }

  def fetch(bucket: String, key: String, resolver: RiakConflictsResolver): Future[Option[RiakValue]] = {
    val request = RiakPBRequest(PBCMsgTypes.RpbGetReq, RpbGetReq(bucket, key))
    sendRequest(request).flatMap {
      case Right(result) =>
        val riakResponse = result.as(RpbGetResp())
        val values = riakResponse.`content`.toList

        values.size match {
          // Empty or single result
          case i if i < 2 =>
            Future.successful(values.headOption.map { v => toRiakValue(v, extractVClock(riakResponse.`vclock`)) })

          // Conflict (multiple values) and there is a VClock to resolve it
          case _ if riakResponse.`vclock`.isDefined =>
            resolveConflict(bucket, key, extractVClock(riakResponse.`vclock`).get, values, resolver).map(Some.apply)

          // No VClock is provided
          case _ =>
            throw new BucketOperationFailed("Received a wrong response from Riak: missing vClock field.")
        }

      case Left(errorMsg) =>
        throw new BucketOperationFailed(s"Fetch for key '$key' in bucket '$bucket' produced an unexpected response error '$errorMsg'.")
    }
  }

  def fetch(bucket: String, index: RiakIndex, resolver: RiakConflictsResolver): Future[List[RiakValue]] = {
    val request = RiakPBRequest(PBCMsgTypes.RpbIndexReq, RpbIndexReq(`bucket` = bucket, `index` = index.fullName, `stream` = Some(false),
      `key` = Some(index.value.toString), `qtype` = RpbIndexReq.IndexQueryType.eq))

    sendRequest(request).flatMap {
      case Right(result) =>
        val riakResponse = result.as(RpbIndexResp())
        Future.traverse(riakResponse.`keys`.toList)(fetch(bucket, _, resolver)).map(_.flatten)
     
      case Left(errorMsg) => 
        throw new BucketOperationFailed(s"Fetch for index '${index.name}' in bucket '$bucket' produced an unexpected response error '$errorMsg'.")      
    }
  }

  def fetch(bucket: String, indexRange: RiakIndexRange, resolver: RiakConflictsResolver): Future[List[RiakValue]] = {
    val request = RiakPBRequest(PBCMsgTypes.RpbIndexReq, RpbIndexReq(`bucket` = bucket, `index` = indexRange.fullName, `stream` = Some(false),
      `rangeMin` = Some(indexRange.start.toString), `rangeMax` = Some(indexRange.end.toString), `qtype` = RpbIndexReq.IndexQueryType.range))

    sendRequest(request).flatMap {
      case Right(result) =>
        val riakResponse = result.as(RpbIndexResp())
        Future.traverse(riakResponse.`keys`.toList)(fetch(bucket, _, resolver)).map(_.flatten)

      case Left(errorMsg) =>
        throw new BucketOperationFailed(s"Fetch for index '${indexRange.name}' in range <${indexRange.start}; ${indexRange.end}> in bucket '$bucket' produced an unexpected response error '$errorMsg'.")
    }
  }  

  def storeAndFetch(bucket: String, key: String, value: RiakValue, resolver: RiakConflictsResolver): Future[RiakValue] = {
    if (value.data.isEmpty) {
      throw new BucketOperationFailed("Empty Riak value data provided") // to be consistent with HTTP API behavior.
    }

    val request = RiakPBRequest(PBCMsgTypes.RpbPutReq, RpbPutReq(`bucket` = bucket, `key` = Some(key),
      `vclock` = toRpbVClock(value.vclock), `returnBody` = Some(true), content = toRbpContent(value)))

    sendRequest(request).flatMap {
      case Right(result) =>
        val riakResponse = result.as(RpbPutResp())
        val values = riakResponse.`content`.toList

        riakResponse.`content`.toList match {
          // Empty response
          case Nil       =>
            throw new BucketOperationFailed(s"Store and fetch of value '$value' for key '$key' in bucket '$bucket' failed: empty response.")

          // Single result
          case v :: Nil  =>
            Future.successful(toRiakValue(v, extractVClock(riakResponse.`vclock`)))

          // Conflict (multiple values) and there is a VClock to resolve it
          case _ if riakResponse.`vclock`.isDefined =>
            resolveConflict(bucket, key, extractVClock(riakResponse.`vclock`).get, values, resolver)

          // No VClock is provided
          case _         =>
            throw new BucketOperationFailed(s"Store and fetch of value '$value' for key '$key' in bucket '$bucket' failed: missing VClock field.")
        }

      case Left(errorMsg) =>
        throw new BucketOperationFailed(s"Store and fetch of value '$value' for key '$key' in bucket '$bucket' failed due to $errorMsg.")
    }
  }

  def store(bucket: String, key: String, value: RiakValue, resolver: RiakConflictsResolver): Future[Unit] = {
    if (value.data.isEmpty) {
      throw new BucketOperationFailed("Empty Riak value data provided") // to be consistent with HTTP API behavior.
    }

    val request = RiakPBRequest(PBCMsgTypes.RpbPutReq, RpbPutReq(`bucket` = bucket, `key` = Some(key),
      `vclock` = toRpbVClock(value.vclock), `returnBody` = Some(false), content = toRbpContent(value)))

    sendRequest(request).map {
      case Right(_)       => ()
      case Left(errorMsg) =>
        throw new BucketOperationFailed(s"Store of value '$value' for key '$key' in bucket '$bucket' failed due to $errorMsg.")
    }
  }

  def delete(bucket: String, key: String): Future[Unit] = {
    val request = RiakPBRequest(PBCMsgTypes.RpbDelReq, RpbDelReq(bucket = bucket, key = key))
    sendRequest(request).map {
      case Right(result)  => ()
      case Left(errorMsg) =>
        throw new BucketOperationFailed(s"Delete for key '$key' in bucket '$bucket' produced an unexpected response code '$errorMsg'.")
    }
  }

  def getBucketProperties(bucket: String): Future[RiakBucketProperties] = {
    val request = RiakPBRequest(PBCMsgTypes.RpbGetBucketReq, RpbGetBucketReq(bucket = bucket))

    sendRequest(request).map {
      case Right(result)  =>
        val riakResponse = result.as(RpbGetBucketResp())
        val props = riakResponse.`props`
        RiakBucketProperties(allowSiblings = props.`allowMult`.getOrElse(DefaultAllowMult),
          lastWriteWins = props.`lastWriteWins`.getOrElse(DefaultLastWriteWins), numberOfReplicas = props.`nVal`.getOrElse(DefaultNumberOfReplicas))

      case Left(errorMsg) =>
        throw new BucketOperationFailed(s"Fetching properties of bucket '$bucket' produced an unexpected response error '$errorMsg'.")
    }
  }

  def setBucketProperties(bucket: String, newProperties: Set[RiakBucketProperty[_]]): Future[Unit] = {
    val request = RiakPBRequest(PBCMsgTypes.RpbSetBucketReq, RpbSetBucketReq(`bucket` = bucket, `props` = toRpbBucketProps(newProperties)))

    sendRequest(request).map {
      case Right(_)       => ()
      case Left(errorMsg) =>
        throw new BucketOperationFailed(s"Setting properties of bucket '$bucket' produced an unexpected response '$errorMsg'.")
    }
  }

  // ==========================================================================
  // Conflict Resolution
  // ==========================================================================

  private def resolveConflict(bucket: String, key: String, vClock: String,
                              contentList: List[RpbContent], resolver: RiakConflictsResolver): Future[RiakValue] = {
    val values = contentList.filterNot(value => value.`deleted`.getOrElse(false)).map(toRiakValue(_, Some(vClock))).toSet

    // Store the resolved value back to Riak and return the resulting RiakValue
    val ConflictResolution(result, writeBack) = resolver.resolve(values)

    if (writeBack) {
      storeAndFetch(bucket, key, result, resolver)
    } else {
      Future.successful(result)
    }
  }

  // ==========================================================================
  // Request building
  // ==========================================================================

  private def sendRequest(request: RiakPBRequest): Future[RiakReply] =
    clientActor.ask(request).mapTo[RiakPBResponse].map(responseToEither)

  // =================================================================================
  // Helper methods to construct Rpb protocol objects from Riak client domain objects
  // =================================================================================

  private def toRbpContent(value: RiakValue): RpbContent =
    new RpbContent().copy(
      `value`           = value.data,
      `contentType`     = Some(value.contentType.mediaType.value),
      `lastMod`         = Some(value.lastModified.getMillis.toInt),
      `contentEncoding` = Some(value.contentType.charset.value),
      `vtag`            = toRpbEtag(value.etag),
      `indexes`         = toRpbPairs(value.indexes))


  private def toRpbPairs(indexes: Set[RiakIndex]): Vector[RpbPair] =
    indexes.map { index =>
      RpbPair(`key` = index.fullName, `value` = Some(index.value.toString))
    }.toVector

  private def toRpbVClock(vClock: VClock): Option[PBByteString] =
    vClock match {
      case VClock.NotSpecified => None
      case _                   => Some(PBByteString.copyFrom(base64.decode(vClock.value)))
    }

  private def toRpbEtag(eTag: ETag): Option[PBByteString] =
    eTag match {
      case ETag.NotSpecified => None
      case _                 => Some(eTag.value)
    }

  private def toRpbBucketProps(properties: Set[RiakBucketProperty[_]]): RpbBucketProps = {
    properties.foldLeft(RpbBucketProps()) { (rbProps, newProperty) =>
      newProperty match {
        case NumberOfReplicas(n) => rbProps.copy(`nVal` = Some(n))
        case AllowSiblings(v)    => rbProps.copy(`allowMult` = Some(v))
        case LastWriteWins(l)    => rbProps.copy(`lastWriteWins` = Some(l))
      }
    }
  }

  // ==========================================================================
  // Response parsing
  // ==========================================================================

  // =================================================================================
  // Helper methods to construct Riak client domain objects from Rpb protocol objects
  // =================================================================================

  private def toRiakValue(content: RpbContent, vClock: Option[String]): RiakValue = {
    def fromStringToContentType(s: String): ContentType = {
      import spray.http._
      spray.http.ContentType(MediaType.custom(s), Some(HttpCharsets.`UTF-8`))
    }

    RiakValue(
      data = content.`value`,
      contentType  = content.`contentType`.map(bs => fromStringToContentType(bs)).getOrElse(ContentTypes.NoContentType),
      vclock       = vClock.map(VClock(_)).getOrElse(VClock.NotSpecified),
      etag         = content.`vtag`.map(ETag(_)).getOrElse(ETag.NotSpecified),
      lastModified = new DateTime(content.`lastMod`.map(_.toLong).getOrElse(System.currentTimeMillis())),
      indexes      = toRiakIndexes(content.`indexes`))
  }

  private def toRiakIndexes(rpbIndexes: Vector[RpbPair]): Set[RiakIndex] = {
    def toRiakIndex(rpbPair: RpbPair) = {
      import RiakIndexSupport._
      val fullName: String = rpbPair.`key`
      val value: Option[String] = rpbPair.`value`

      fullName match {
        case IndexNameAndType(name, LongIndexSuffix)   => RiakLongIndex(name, value.get.toLong)
        case IndexNameAndType(name, StringIndexSuffix) => RiakStringIndex(name, value.get)
      }
    }

    rpbIndexes.filter(_.`value`.isDefined)
              .map(toRiakIndex)
              .toSet
  }

  private def extractVClock(vClock: Option[PBByteString]): Option[String] =
    vClock.map(_.toByteArray).map(base64.encodeToString(_, false))
}
