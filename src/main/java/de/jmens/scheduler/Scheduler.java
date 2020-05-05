package de.jmens.scheduler;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.stream.IntStream;

public class Scheduler<Task extends Runnable, Key> implements AutoCloseable {

  private Map<Key, Queue<Task>> tasksByKey = new ConcurrentHashMap<>();
  private Queue<Queue<Task>> taskGroups = new ConcurrentLinkedQueue<>();

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

    if (running) {
      nextFreeExecutor().ifPresent(executor -> executor.submit(task));
    }

    return this;
  }

  private Optional<ThreadPoolExecutor> nextFreeExecutor() {
    for (int i = 0; i < executors.size(); i++) {
      lastElectedExecutor = (lastElectedExecutor + 1) % executors.size();
      final var executor = executors.get(lastElectedExecutor);
      if (executor.getPoolSize() < maximumPoolSize) {
        return Optional.of(executor);
      }
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

  @Override
  public void close() {
    this.executors.forEach(executor -> executor.shutdown());
  }
}
