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

package com.scalapenos.riak.internal.protobuf.serialization

import com.google.protobuf.{ByteString => PBByteString, CodedOutputStream, MessageLite}
import java.io.ByteArrayOutputStream
import com.basho.riak.protobuf.{RpbErrorResp, Message}
import akka.util.ByteString
import com.scalapenos.riak.internal.protobuf.{PBCMsgTypes, RiakPBResponse}
import scala.util.Try
import java.lang.Exception

object PBConversions {

  implicit def messageToByteString[T <: MessageLite with MessageLite.Builder](m: Message[T]) = {
    val os = new ByteArrayOutputStream()
    val cos = CodedOutputStream.newInstance(os)
    m.writeTo(cos)
    cos.flush()
    os.close()
    ByteString(os.toByteArray)
  }

  implicit def stringToByteString(s: String): PBByteString = PBByteString.copyFromUtf8(s)

  implicit def byteStringToString(s: PBByteString): String = s.toStringUtf8

  def responseToTry(r: RiakPBResponse): Try[RiakPBResponse] =
    r.msgType match {
      case PBCMsgTypes.RpbErrorResp =>
        val e = new Exception(RpbErrorResp().mergeFrom(r.body.toArray).errmsg)
        scala.util.Failure(e)

      case _                        => scala.util.Success(r)
    }
}
