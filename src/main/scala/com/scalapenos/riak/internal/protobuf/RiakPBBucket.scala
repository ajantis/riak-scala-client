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

import com.scalapenos.riak.{RiakBucket, RiakBucketProperty, RiakValue, RiakIndex}
import com.scalapenos.riak.internal.RiakIndexRange

private[riak] final class RiakPBBucket(val helper: RiakPBClientHelper,
                                       val name: String,
                                       val resolver: RiakConflictsResolver) extends RiakBucket {

  def fetch(key: String) = helper.fetch(name, key, resolver)
  def storeAndFetch(key: String, value: RiakValue) = helper.storeAndFetch(name, key, value, resolver)
  def delete(key: String) = helper.delete(name, key)

  def fetch(index: RiakIndex) = null // TODO
  def fetch(indexRange: RiakIndexRange) = null // TODO
  def store(key: String, value: RiakValue) = null // TODO
  def properties = null // TODO
  def properties_=(newProperties: Set[RiakBucketProperty[_]]) = null // TODO
}
