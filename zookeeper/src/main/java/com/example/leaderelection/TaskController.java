package com.example.leaderelection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class TaskController {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskController.class);

  private ExecutorService executorService = Executors.newFixedThreadPool(10);
  private List<Future> futures = new ArrayList<>();

  public void start(List<Task> tasks) {
    futures = tasks.stream().map(task -> executorService.submit(task)).collect(Collectors.toList());
  }

  public void stop() {
    futures.stream().forEach(future -> {
      boolean result = future.cancel(true);
      if (!result) {
        LOGGER.info("Canceling a task failed!");
      }
    });
//    executorService.shutdownNow();
  }
}
