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

import akka.actor._
import com.scalapenos.riak.internal.protobuf.{RiakPBClient, RiakPBClientHelper}
import com.scalapenos.riak.RiakClient.{ProtoBuf, Http, Protocol}


// ============================================================================
// RiakClient - The main entry point
// ============================================================================

object RiakClient {
  private val defaultHost = "http://localhost:8098"

  private lazy val internalSystem = ActorSystem("riak-client")

  def apply()                                                                  : RiakClient = RiakClientExtension(internalSystem).connect(defaultHost, Http)
  def apply(protocol: Protocol)                                                : RiakClient = RiakClientExtension(internalSystem).connect(defaultHost, protocol)
  def apply(host: String, port: Int, protocol: Protocol)                       : RiakClient = RiakClientExtension(internalSystem).connect(host, port, protocol)
  def apply(url: String, protocol: Protocol)                                   : RiakClient = RiakClientExtension(internalSystem).connect(url, protocol)
  def apply(url: java.net.URL, protocol: Protocol)                             : RiakClient = RiakClientExtension(internalSystem).connect(url, protocol)

  def apply(system: ActorSystem, protocol: Protocol = ProtoBuf)                : RiakClient = RiakClientExtension(system).connect(defaultHost, protocol)
  def apply(system: ActorSystem, host: String, port: Int, protocol: Protocol)  : RiakClient = RiakClientExtension(system).connect(host, port, protocol)
  def apply(system: ActorSystem, url: String, protocol: Protocol)              : RiakClient = RiakClientExtension(system).connect(url, protocol)
  def apply(system: ActorSystem, url: java.net.URL, protocol: Protocol)        : RiakClient = RiakClientExtension(system).connect(url, protocol)

  sealed abstract class Protocol
  case object Http extends Protocol
  case object ProtoBuf extends Protocol
}

trait RiakClient {
  import scala.concurrent.Future

  // TODO: stats

  def ping: Future[Boolean]
  def bucket(name: String, resolver: RiakConflictsResolver = DefaultConflictsResolver): RiakBucket
}


// ============================================================================
// RiakClientExtension - The root of the actor tree
// ============================================================================

object RiakClientExtension extends ExtensionId[RiakClientExtension] with ExtensionIdProvider {
  def lookup() = RiakClientExtension
  def createExtension(system: ExtendedActorSystem) = new RiakClientExtension(system)
}

class RiakClientExtension(system: ExtendedActorSystem) extends Extension {
  import internal._

  private val defaultHttpPort = 8098
  private val defaultPbPort = 8087

  private[riak] val settings = new RiakClientSettings(system.settings.config)
  private[riak] lazy val httpHelper = new RiakHttpClientHelper(system)
  private[riak] def pbcHelper(server: RiakServerInfo) = new RiakPBClientHelper(system, server)

  def connect(url: String, protocol: Protocol): RiakClient = connect(RiakServerInfo(url), protocol)
  def connect(url: java.net.URL, protocol: Protocol): RiakClient = connect(RiakServerInfo(url), protocol)
  def connect(host: String, port: Int, protocol: Protocol): RiakClient = connect(serverInfo(host, port, protocol), protocol)

  private def connect(server: RiakServerInfo, protocol: Protocol): RiakClient =
    protocol match {
      case Http     => new RiakHttpClient(httpHelper, server)
      case ProtoBuf => new RiakPBClient(pbcHelper(server))
    }

  private def serverInfo(host: String, port: Int, protocol: Protocol): RiakServerInfo = {
    protocol match {
      case ProtoBuf => RiakServerInfo(host, pbPort = port, httpPort = defaultHttpPort)
      case Http     => RiakServerInfo(host, httpPort = port, pbPort = defaultPbPort)
    }
  }
}
