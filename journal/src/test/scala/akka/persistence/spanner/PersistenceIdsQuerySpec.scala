/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.spanner

import akka.Done
import akka.persistence.query.PersistenceQuery
import akka.persistence.spanner.TestActors.Persister.PersistMe
import akka.persistence.spanner.scaladsl.SpannerReadJournal
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink

class PersistenceIdsQuerySpec extends SpannerSpec {
  val query = PersistenceQuery(testKit.system)
    .readJournalFor[SpannerReadJournal](SpannerReadJournal.Identifier)

  val pid1 = nextPid
  val pid2 = nextPid
  val pid3 = nextPid
  val probe = testKit.createTestProbe[Done]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    testKit.spawn(TestActors.Persister(pid1)) ! PersistMe("cat", probe.ref)
    probe.expectMessage(Done)
    testKit.spawn(TestActors.Persister(pid2)) ! PersistMe("dog", probe.ref)
    probe.expectMessage(Done)
    testKit.spawn(TestActors.Persister(pid3)) ! PersistMe("giraffe", probe.ref)
    probe.expectMessage(Done)
  }

  "currentPersistenceIds" must {
    "work" in {
      val pids = query.currentPersistenceIds().runWith(Sink.seq).futureValue
      pids.size shouldEqual 3
      pids.toSet shouldEqual Set(pid1, pid2, pid3)
    }
    "work with paging" in {
      val store = PersistenceQuery(testKit.system).readJournalFor[SpannerReadJournal](SpannerReadJournal.Identifier)
      val all = store
        .currentPersistenceIds(None, 1000)
        .runWith(Sink.seq)
        .futureValue

      all.size shouldBe >(2)
      val firstThree = store
        .currentPersistenceIds(None, 2)
        .runWith(Sink.seq)
        .futureValue
      val others = store
        .currentPersistenceIds(Some(firstThree.last), 1000)
        .runWith(Sink.seq)
        .futureValue

      (firstThree ++ others) should contain theSameElementsAs (all)
    }
  }

  "live persistenceIds" must {
    "show new persistence ids" in {
      val probe = testKit.createTestProbe[Done]()

      val pids = query.persistenceIds().runWith(TestSink.probe)
      pids.request(10)
      pids.expectNextN(3).toSet shouldEqual Set(pid1, pid2, pid3) // the three from setup
      pids.expectNoMessage()

      val pid4 = nextPid
      val pid5 = nextPid
      testKit.spawn(TestActors.Persister(pid4)) ! PersistMe("cat", probe.ref)
      testKit.spawn(TestActors.Persister(pid5)) ! PersistMe("dog", probe.ref)

      // should not get pid1, pid2 and pid3 again
      pids.expectNextN(2).toSet shouldEqual Set(pid4, pid5)
      pids.expectNoMessage()
      pids.cancel()
    }
  }
}
