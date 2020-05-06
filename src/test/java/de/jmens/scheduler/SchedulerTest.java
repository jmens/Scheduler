package de.jmens.scheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.slf4j.LoggerFactory.getLogger;

import de.jmens.scheduler.test.TestTask;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

class SchedulerTest {

  private static final Logger LOGGER = getLogger(TestTask.class);

  private Scheduler<TestTask, String> scheduler;

  @BeforeEach
  void setupTest() {
    this.scheduler = new Scheduler<>(1, TestTask::getKey);
  }

  @Test
  @DisplayName("Add tasks to halted scheduler")
  void testTasksInHaltedScheduler() {

    LOGGER.info("Adding 100 tasks to the halted scheduler");

    IntStream.range(0, 100).boxed().map(i -> new TestTask()).forEach(task -> scheduler.add(task));

    assertThat("Current task load should be 100", scheduler.countKeys(), is(100));
  }

  @Test
  @DisplayName("Basic execution of tasks")
  void testExecution() {

    LOGGER.info("Starting scheduler");

    scheduler.start();

    IntStream.range(0, 100)
        .parallel()
        .boxed()
        .map(i -> new TestTask())
        .peek(task -> scheduler.add(task))
        .forEach(task -> assertThat("Task should be executed", task.isExecuted(), is(true)));
  }

  @Test
  @DisplayName("Exceed schedulers task pool size")
  void testExceedExecutorCapacity() {

    LOGGER.info("Starting scheduler");

    scheduler.updateMaximumPoolSize(5).start();

    IntStream.range(0, 10)
        .boxed()
        .parallel()
        .map(
            i ->
                new TestTask() {
                  @Override
                  public void run() {
                    sleep(100);
                    super.run();
                  }
                })
        .peek(task -> scheduler.add(task))
        .forEach(task -> assertThat("Task should be executed", task.isExecuted(), is(true)));
  }

  private static final void sleep(int milliseconds) {
    try {
      Thread.sleep(milliseconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
