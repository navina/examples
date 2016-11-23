package com.example.leaderelection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker {
  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

  private final ZkUtils zkUtils;
  private final ZkController controller;
  private final int workerId;
  private final TaskController taskController = new TaskController();

  public Worker (int workerId, String pathPrefix, String zkConnect) {
    this.workerId = workerId;
    this.zkUtils = new ZkUtils(pathPrefix, zkConnect);

    this.controller = new ZkController(workerId, zkUtils, taskController);
  }

  public void start() {
    controller.start();
  }

  public void stop() {
    controller.stop();
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      throw new RuntimeException("Need at least 2 arguments - zkConnectionString and workerId");
    }

    final String pathPrefix = "group";

    String zkString = args[0];
    initZk(pathPrefix, zkString);

    int workerId = Integer.parseInt(args[1]);
    final Worker worker = new Worker(workerId, pathPrefix, zkString);
    worker.start();

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        LOGGER.info("Shutting the JVM");
        LOGGER.info("Shutting down the worker!");
        worker.stop();
      }
    }));

    try {
      Thread.sleep(Integer.MAX_VALUE);
    } catch (InterruptedException e) {
      e.printStackTrace();
      worker.stop();
    }
  }

  private static void initZk(String pathPrefix, String connectionString) {
    ZkUtils utils = new ZkUtils(pathPrefix, connectionString);
    utils.makeSurePersistentPathExists(utils.getProcessorsPath());
    utils.makeSurePersistentPathExists(utils.getAssignmentPath());
    utils.makeSurePersistentPathExists(utils.getStatusPath());
    utils.close();
  }
}
