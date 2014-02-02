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
package serialization

import com.google.protobuf.{ByteString => PBByteString, CodedOutputStream, MessageLite}
import com.basho.riak.protobuf.{Message, RpbErrorResp}

import java.io.ByteArrayOutputStream

object PBConversions {
  import akka.util.ByteString

  implicit def messageToByteString[T <: MessageLite with MessageLite.Builder](m: Message[T]) = {
    val os = new ByteArrayOutputStream()
    val cos = CodedOutputStream.newInstance(os)
    m.writeTo(cos)
    cos.flush()
    os.close()
    ByteString(os.toByteArray)
  }

  implicit def stringToByteString(string: String): PBByteString = PBByteString.copyFromUtf8(string)

  implicit def byteStringToString(byteString: PBByteString): String = byteString.toStringUtf8

  implicit def optionByteStringToOptionString(byteStringOpt: Option[PBByteString]): Option[String] = byteStringOpt.map(byteStringToString)

  /**
   * Wraps a response to Either monad base on it's message type.
   * @param response a Riak PB response to convert.
   * @return a `Left[String]` with error msg if response message type is Error or Right[RiakPBResponse] in other case.
   */
  def responseToEither(response: RiakPBResponse): Either[String, RiakPBResponse] =
    response.msgType match {
      case PBCMsgTypes.RpbErrorResp => Left(RpbErrorResp().mergeFrom(response.body.toArray).errmsg)
      case _                        => Right(response)
    }
}
