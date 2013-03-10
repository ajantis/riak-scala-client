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

import scala.concurrent._
import scala.concurrent.Future._
import scala.concurrent.duration._
import scala.util._

import spray.json.DefaultJsonProtocol._

import akka.actor._


class ConflictResolutionSpec extends RiakClientSpecification with RandomKeySupport {

  case class TestEntityWithMergableList(things: List[String])
  object TestEntityWithMergableList {
    implicit val jsonFormat = jsonFormat1(TestEntityWithMergableList.apply)
  }

  case object TestEntityWithMergableListResolver extends ConflictResolver {
    // this resolver merges the lists of things and removes any duplicates
    def resolve(values: Set[RiakValue]) = {
      val entities = values.map(_.asMeta[TestEntityWithMergableList])

      val mergedThings = entities.foldLeft(Set[String]()) { (merged, entity) =>
         merged ++ entity.data.things.toSet
      }

      entities.head
              .map(_.copy(things = mergedThings.toList))
              .toRiakValue
    }
  }

  "When dealing with concurrent writes, a bucket configured with allow_mult = true and the default resolver" should {
    "resolve any conflicts, store the resolved value back to Riak, and return the result" in {
      pending
    }
  }

  "When dealing with concurrent writes, a bucket configured with allow_mult = true and a custom resolver" should {
    "resolve any conflicts, store the resolved value back to Riak, and return the result" in {
      val bucket = client.bucket("riak-conflict-resolution-tests-" + randomKey, TestEntityWithMergableListResolver)
      val key = randomKey

      bucket.setAllowSiblings(true).await
      bucket.allowSiblings.await must beTrue

      val things = List("one", "two", "five")
      val updatedThings1 = List("one", "three")
      val updatedThings2 = List("two", "four")

      val entity = TestEntityWithMergableList(things)

      val storedValue = bucket.store(key, entity, returnBody = true).await.get
      val storedMeta = storedValue.asMeta[TestEntityWithMergableList]

      // concurrent writes based on the same vclock
      bucket.store(key, storedMeta.map(_.copy(updatedThings1))).await
      bucket.store(key, storedMeta.map(_.copy(updatedThings2))).await

      val resolvedValue = bucket.fetch(key).await
      val resolvedMeta = resolvedValue.get.asMeta[TestEntityWithMergableList]

      resolvedMeta.data.things must containTheSameElementsAs(updatedThings1 ++ updatedThings2)

      bucket.fetch(key).await must beEqualTo(resolvedValue)
    }
  }

}
