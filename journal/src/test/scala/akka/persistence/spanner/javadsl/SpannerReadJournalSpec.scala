/*
 * Copyright (C) 2021 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.spanner.javadsl

import akka.persistence.query.PersistenceQuery
import akka.persistence.spanner.SpannerSpec

class SpannerReadJournalSpec extends SpannerSpec {
  "SpannerReadJournal" should {
    "load" in {
      PersistenceQuery(testKit.system).getReadJournalFor(classOf[SpannerReadJournal], SpannerReadJournal.Identifier)
    }
  }
}
