package akka.persistence.spanner.testkit;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class SpannerTestkitTest {

  @ClassRule
  public static final TestKitJunitResource testKit =
      new TestKitJunitResource(SpannerTestkitSpec.config());

  //#setup
  public static final SpannerTestkit spannerTestkit = new SpannerTestkit(testKit.system());

  @BeforeClass
  public static void createDatabase() {
    spannerTestkit.createDatabaseAndSchema();
  }

  @AfterClass
  public static void dropDatabase() {
    spannerTestkit.dropDatabase();
  }
  //#setup

  @Test
  public void shouldHaveCreatedTheDatabase() {
    TestProbe<SpannerTestkitSpec.Pong> testProbe = testKit.createTestProbe();
    ActorRef<SpannerTestkitSpec.Ping> ref = testKit.spawn(SpannerTestkitSpec.pingPong());
    ref.tell(new SpannerTestkitSpec.Ping(testProbe.ref()));
    testProbe.expectMessage(new SpannerTestkitSpec.Pong());
  }
}
