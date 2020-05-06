package de.jmens.scheduler;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.slf4j.Logger;

public class Scheduler<Task extends Runnable, Key> implements AutoCloseable {

  private static final Logger LOGGER = getLogger(Scheduler.class);

  private Map<Key, Queue<Task>> tasksByKey = new ConcurrentHashMap<>();

  private List<Queue<Task>> taskGroups = new CopyOnWriteArrayList<>();
  private int lastElectedTaskGroup;

  private Function<Task, Key> keyExtractor;

  private int maximumPoolSize = Integer.MAX_VALUE;

  private List<ThreadPoolExecutor> executors;
  private int lastElectedExecutor;

  private Function<Integer, ThreadPoolExecutor> executorFactory;

  private boolean running;

  public Scheduler(final int capacity, final Function<Task, Key> keyExtractor) {
    this(
        capacity,
        keyExtractor,
        index -> new ThreadPoolExecutor(1, 1, 0, SECONDS, new LinkedBlockingQueue<>()));
  }

  public Scheduler(
      final int capacity,
      final Function<Task, Key> keyExtractor,
      final Function<Integer, ThreadPoolExecutor> executorFactory) {

    if (capacity <= 0) {
      throw new IllegalStateException("Capacity must be non-zero and non-negative");
    }

    this.executors = new ArrayList<>(capacity);

    this.executorFactory = executorFactory;

    this.keyExtractor = keyExtractor;

    IntStream.range(0, capacity).forEach(index -> executors.add(this.executorFactory.apply(index)));
  }

  public Scheduler<Task, Key> add(final Task task) {

    // Generate key for this task
    final var key = keyExtractor.apply(task);

    LOGGER.trace("Submitting task {} to scheduler", key);

    // Create queue for this key, if not already existing
    final var queue =
        tasksByKey.computeIfAbsent(
            key,
            unused -> {
              final var result = new ConcurrentLinkedQueue<Task>();
              taskGroups.add(result);
              return result;
            });

    // Add task to its queue
    queue.add(task);

    submit(task);

    return this;
  }

  private void submitEligibleTasks() {
    /** TODO: Mark task as submitted, see {@link #submit(Task task)} */
    Optional<ThreadPoolExecutor> executor;
    while ((executor = nextFreeExecutor()).isPresent()) {
      final var task = nextEligibleTask();
      if (task.isEmpty()) {
        LOGGER.info("No more eligible tasks available.");
        return;
      }
      LOGGER.info("Submitting task {} to executor", keyExtractor.apply(task.get()));
      executor.get().submit(feedbacking(task.get()));
    }
    LOGGER.info("No more free executors availale");
  }

  private void submit(final Task task) {
    // TODO: Mark task as submitted
    // Implement marker in FeedbackingTask and wrap Tasks when submitted to this scheduler
    if (running)
      synchronized (Scheduler.class) {
        nextFreeExecutor().ifPresent(executor -> executor.submit(feedbacking(task)));
      }
  }

  private void remove(final Task task) {
    // TODO: Null safety
    LOGGER.info("Removing task {} from scheduler", keyExtractor.apply(task));
    tasksByKey.get(keyExtractor.apply(task)).remove(task);
  }

  private Runnable feedbacking(final Task task) {
    return new FeedbackingTask<>(task, this);
  }

  private Optional<ThreadPoolExecutor> nextFreeExecutor() {
    LOGGER.trace("Executor requested, executor pool size is {}", executors.size());
    for (int i = 0; i < executors.size(); i++) {
      lastElectedExecutor = (lastElectedExecutor + 1) % executors.size();
      final var executor = executors.get(lastElectedExecutor);
      if (executor.getQueue().size() < maximumPoolSize) {
        LOGGER.trace("Eligble executor found: {}", executor);
        return Optional.of(executor);
      }
    }
    LOGGER.trace("No eligible executor found, all {} executors are saturated", executors.size());
    return Optional.empty();
  }

  private Optional<Task> nextEligibleTask() {
    // TODO: Null safety
    for (int i = 0; i < taskGroups.size(); i++) {
      lastElectedTaskGroup = (lastElectedTaskGroup + 1) % taskGroups.size();
      return Optional.of(taskGroups.get(lastElectedTaskGroup).peek());
    }
    return Optional.empty();
  }

  public Scheduler<Task, Key> start() {
    this.running = true;
    return this;
  }

  public int countKeys() {
    return taskGroups.size();
  }

  public Scheduler<Task, Key> updateMaximumPoolSize(final int limit) {
    this.maximumPoolSize = limit;
    return this;
  }

  @Override
  public void close() {
    this.executors.forEach(executor -> executor.shutdown());
  }

  synchronized void taskFinished(final Task task) {
    LOGGER.info("Task {} finished", keyExtractor.apply(task));
    remove(task);
    submitEligibleTasks();
  }
}
