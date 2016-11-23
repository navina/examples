package com.example.leaderelection;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ZkUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkUtils.class);

  public static final String WORKERS_PATH = "workers";
  public static final String ASSIGNMENT_PATH = "assign";
  public static final String STATUS_PATH = "status";
  public final ReentrantLock lock = new ReentrantLock();

  private String pathPrefix = "";

  private final ZkClient zkClient;
  private final ZkConnection zkConnnection;

  public ZkUtils(String zkConnectString) {
    this ("", zkConnectString);
  }

  public ZkUtils(String pathPrefix, String zkConnectString) {
    this.zkConnnection = new ZkConnection(zkConnectString, 60000);
    this.zkClient = new ZkClient(zkConnnection, 60000);
    this.pathPrefix = pathPrefix;
  }

  public String getProcessorsPath() {
    return String.format("/%s/%s", pathPrefix, WORKERS_PATH);
  }

  public String getAssignmentPath() {
    return String.format("/%s/%s", pathPrefix, ASSIGNMENT_PATH);
  }

  public String getStatusPath() {
    return String.format("/%s/%s", pathPrefix, STATUS_PATH);
  }


  public ZkClient getZkClient() {
    return zkClient;
  }

  public ZkConnection getZkConnnection() {
    return zkConnnection;
  }

  public void makeSurePersistentPathExists(String path) {
    if (!zkClient.exists(path)) {
      zkClient.createPersistent(path, true);
    }
  }

  public static String parseWorkerIdFromPath(String path) {
    return path.substring(path.indexOf("worker-"));
  }

  public String registerAsWorker() {
    try {
      String ephemeralPath =
          zkClient.createEphemeralSequential(getProcessorsPath() + "/worker-", InetAddress.getLocalHost().getHostName());
      registerForTaskAssignment(ZkUtils.parseWorkerIdFromPath(ephemeralPath));
      return ephemeralPath;
    } catch (UnknownHostException e) {
      throw new RuntimeException("Failed to register as worker. Aborting...");
    }
  }

  public void registerForTaskAssignment(String workerId) {
    zkClient.createEphemeral(String.format("%s/%s", getAssignmentPath(), workerId));
  }

  public void assignTasks(String workerPath, String taskList) {
    zkClient.writeData(
        String.format("%s/%s", getAssignmentPath(), ZkUtils.parseWorkerIdFromPath(workerPath)),
        taskList);
  }

  public String getAssignedTasks(String workerPath) {
    return zkClient.readData(
        String.format("%s/%s", getAssignmentPath(), ZkUtils.parseWorkerIdFromPath(workerPath)),
        true
    );
  }

  public void setTaskStatus(String status) {
    zkClient.writeData(getStatusPath(), status);
  }

  public List<String> findActiveWorkers() {
    List<String> children = zkClient.getChildren(getProcessorsPath());
    assert children.size() > 0;
    Collections.sort(children);
    LOGGER.info("Found these children - " + children);
    return children;
  }

  public void subscribeToWorkerChange(IZkChildListener listener) {
    zkClient.subscribeChildChanges(getProcessorsPath(), listener);
  }


  public void unsubscribeToWorkerChange(IZkChildListener listener) {
    zkClient.unsubscribeChildChanges(getProcessorsPath(), listener);
  }

  public void subscribeToTaskAssignmentChange(String workerPath, IZkDataListener listener) {
    String workerId = ZkUtils.parseWorkerIdFromPath(workerPath);
    zkClient.subscribeDataChanges(String.format("%s/%s", getAssignmentPath(), workerId), listener);
  }

  public void unSubscribeToTaskAssignmentChange(String workerPath, IZkDataListener listener) {
    String workerId = ZkUtils.parseWorkerIdFromPath(workerPath);
    zkClient.unsubscribeDataChanges(String.format("%s/%s", getAssignmentPath(), workerId), listener);
  }

  public void subscribeToTaskStatus(IZkDataListener listener) {
    zkClient.subscribeDataChanges(getStatusPath(), listener);
  }
  /* Wrapper for standard I0Itec methods */
  public void unsubscribeDataChanges(String path, IZkDataListener dataListener) {
    zkClient.unsubscribeDataChanges(path, dataListener);
  }

  public void subscribeDataChanges(String path, IZkDataListener dataListener) {
    zkClient.subscribeDataChanges(path, dataListener);
  }

  public boolean exists(String path) {
    return zkClient.exists(path);
  }

  public void close() {
    zkClient.close();
  }
}
