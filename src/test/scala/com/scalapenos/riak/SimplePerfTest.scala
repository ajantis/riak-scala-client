package com.scalapenos.riak

import spray.util._
import akka.actor.ActorSystem
import com.scalapenos.riak.Protocol.{ProtoBuf, Http}

object SimplePerfTest extends App {
  val system = ActorSystem()

  val sampleText =
    "Channel Islands, archipelago in the English Channel, west of the Cotentin peninsula of France, at the entrance to the Gulf of Saint-Malo, 80 miles (130 km) south of the English coast. The islands are dependencies of the British crown (and not strictly part of the United Kingdom), having been so attached since the Norman Conquest of 1066, when they formed part of the duchy of Normandy. They comprise four main islands, Jersey, Guernsey, Alderney, and Sark, with lesser islets and a labyrinth of rocks and reefs. They are administered according to local laws and customs, being grouped into two distinct bailiwicks of Guernsey and Jersey, with differing constitutions. Alderney, Sark, Herm, Jethou, Lihou, and Brecqhou are Guernseys dependencies, and the Ecrehous rocks and Les Minquiers are Jerseys. The last two were the source of long-standing dispute between England and France until 1953, when the International Court of Justice confirmed British sovereignty. In the late 20th century the dispute revived, as sovereignty of these islands determines allocation of rights to economic development (specifically, petroleum) of the continental shelf."

  def singleTest(protocol: Protocol): Unit = {
    val connection = RiakClient(system, protocol)
    val bucket = connection.bucket("test-basic-interaction")

    bucket.delete("foo").await

    val fetchBeforeStore = bucket.fetch("foo")

    assert(fetchBeforeStore.await == None)

    val storedValue = bucket.storeAndFetch("foo", sampleText).await

    assert(storedValue.data == sampleText)

    val fetchAfterStore = bucket.fetch("foo").await

    assert(fetchAfterStore.isDefined)
    assert(fetchAfterStore.get.data == sampleText)

    assert(bucket.delete("foo").await == ())

    val fetchAfterDelete = bucket.fetch("foo").await

    assert(fetchAfterDelete == None)
  }

  def doTest(protocol: Protocol): (Protocol, Long) = {
    val t1 = System.currentTimeMillis()

    println(s"Executing tests for ${protocol.toString}...")

    (1 to 10).par.map(_ => singleTest(protocol))

    val t2 = System.currentTimeMillis()
    val result = t2 - t1

    protocol -> result
  }

  val tests = doTest(Http) :: doTest(ProtoBuf) :: Nil

  println(" ___________________________________ ")
  println("|---- Protocol ----|----- Time -----|")
  tests.foreach { pair =>
    println(s"|---- ${pair._1.toString.padTo(9, " ").mkString("")}----|---- ${(pair._2.toString + " ms").padTo(4, " ").mkString("")} ----|")
  }

  system.shutdown()
}
