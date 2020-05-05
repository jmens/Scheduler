package de.jmens.scheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

class SchedulerTest {

  private Scheduler<TestTask, String> scheduler;

  public static class TestTask implements Runnable {

    private static final Logger LOGGER = getLogger(TestTask.class);

    private String id = UUID.randomUUID().toString();

    private CompletableFuture<Boolean> executed = new CompletableFuture<>();

    @Override
    public void run() {
      executed.complete(true);
      LOGGER.trace("Task {} executed", id);
    }

    String getKey() {
      return id;
    }

    boolean isExecuted() {
      try {
        return executed.get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        return false;
      }
    }
  }

  @BeforeEach
  void setupTest() {
    this.scheduler = new Scheduler<>(1, TestTask::getKey);
  }

  @Test
  void testAddTask() {
    IntStream.range(0, 100).boxed().map(i -> new TestTask()).forEach(task -> scheduler.add(task));

    assertThat(scheduler.countKeys(), is(100));
  }

  @Test
  void testExecution() {
    scheduler.start();
    IntStream.range(0, 100)
        .boxed()
        .map(i -> new TestTask())
        .peek(task -> scheduler.add(task))
        .forEach(task -> assertThat(task.isExecuted(), is(true)));
  }
}
