package de.jmens.scheduler;

public class FeedbackingTask<Task extends Runnable> implements Runnable {

  private final Task task;
  private final Scheduler scheduler;

  public FeedbackingTask(final Task task, final Scheduler scheduler) {
    this.task = task;
    this.scheduler = scheduler;
  }

  @Override
  public void run() {
    try {
      this.task.run();
    } finally {
      scheduler.taskFinished(task);
    }
  }
}
