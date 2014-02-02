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

import akka.util.ByteString
import com.basho.riak.protobuf.Message
import com.google.protobuf.MessageLite

private[riak] sealed trait RiakPBMessage {
  def msgType: PBCMsgType

  def body: ByteString

  def as[S <: MessageLite with MessageLite.Builder](obj: Message[S]): S = obj.mergeFrom(body.toArray)
}

private[riak] case class RiakPBRequest(msgType: PBCMsgType, body: ByteString = ByteString()) extends RiakPBMessage
private[riak] case class RiakPBResponse(msgType: PBCMsgType, body: ByteString = ByteString()) extends RiakPBMessage

private[riak] sealed trait PBCMsgType

private[riak] object PBCMsgTypes {
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

  def msgType(code: Int): PBCMsgType =
    values.toSeq.find(_._2 == code).map(_._1).getOrElse(throw new IllegalArgumentException("PDC code not found"))
}
