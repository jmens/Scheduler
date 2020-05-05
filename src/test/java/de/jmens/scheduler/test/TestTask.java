package de.jmens.scheduler.test;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

public class TestTask implements Runnable {

  private static final Logger LOGGER = getLogger(TestTask.class);

  private String id = UUID.randomUUID().toString();

  private CompletableFuture<Boolean> executed = new CompletableFuture<>();

  @Override
  public void run() {
    executed.complete(true);
    LOGGER.info("Task {} executed", id);
  }

  public String getKey() {
    return id;
  }

  public boolean isExecuted() {
    try {
      return executed.get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      return false;
    }
  }
}
