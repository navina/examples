package com.example.leaderelection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);
  private final int multiples;
  private final int workerId;

  public Task (int workerId, int multiples) {
    this.workerId = workerId;
    this.multiples = multiples;
  }

  /**
   * When an object implementing interface <code>Runnable</code> is used
   * to create a thread, starting the thread causes the object's
   * <code>run</code> method to be called in that separately executing
   * thread.
   * <p/>
   * The general contract of the method <code>run</code> is that it may
   * take any action whatsoever.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    for (int i = 0; i < Integer.MAX_VALUE; i++) {
      LOGGER.info("[Worker-" +  workerId + "] " + i + " * " + multiples + " = " + (i*multiples));
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
        return;
      }
    }
  }

  @Override
  public String toString() {
    return " " + String.valueOf(multiples) + " ";
  }
}
