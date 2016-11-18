package com.example.leaderelection;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Driver {
  private static final Logger LOGGER = LoggerFactory.getLogger(Driver.class);

  public static void main(String[] args) {
      if (args.length < 2) {
        throw new RuntimeException("Need at least 2 arguments - zkConnectionString and workerId");
      }

      String zkString = args[0];
      int workerId = Integer.parseInt(args[1]);
      ZkClient zkClient = new ZkClient(zkString, 60000);
      if (!zkClient.exists(ZkUtils.PROCESSORS_PATH)) {
        zkClient.createPersistent(ZkUtils.PROCESSORS_PATH);
      }
      zkClient.close();

      final Worker worker = new Worker(workerId, ZkUtils.PROCESSORS_PATH, new ZkClient(zkString, 60000));
      worker.startUp();
      worker.joinElection();

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
}
