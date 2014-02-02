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

import com.typesafe.config.Config
import scala.concurrent.duration._


private[riak] class RiakClientSettings(config: Config) {

  /**
   * Setting for controlling whether the Riak client should add the
   * X-Riak-ClientId http header to all outgoing http requests.
   *
   * The value of the X-Riak-ClientId header will be UUID.randomUUID().toString
   * and will only be set once per instance of the RiakClientExtension (i.e.
   * per ActorSystem).
   *
   * This value defaults to false.
   */
  final val AddClientIdHeader: Boolean = config.getBoolean("riak.add-client-id-header")

  /**
   * Default timeout to use for futures.
   */
  final val DefaultFutureTimeout = FiniteDuration(config.getMilliseconds("timeouts.default-future-timeout"), MILLISECONDS)

  /**
   * Minimum number of allowed protobuf connections.
   */
  final val MinNumberProtobufConnections = config.getInt("protobuf.min-concurrent-connections")

  /**
   * Maximum number of allowed protobuf connections.
   */
  final val MaxNumberProtobufConnections = config.getInt("protobuf.max-concurrent-connections")

  /**
   * Number of requests queued to Protobuf client before resizing connections pool.
   */
  final val ProtobufQueueBeforeResizingPool = config.getInt("protobuf.queue-before-resizing-pool")

  /**
   * Number of connections retries for Protobuf client.
   */
  final val MaxNumberOfProtobufRetries = config.getInt("protobuf.max-retries")

  /**
   * Timeout after which Protobuf idle connections are expired.
   */
  final val ProtobufConnectionIdleTimeout = FiniteDuration(config.getMilliseconds("protobuf.connection-idle-timeout"), MILLISECONDS)

  // TODO: add setting for silently ignoring indexes on backends that don't allow them. The alternative is failing/throwing exceptions

}
