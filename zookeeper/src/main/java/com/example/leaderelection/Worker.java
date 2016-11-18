package com.example.leaderelection;

import com.example.leaderelection.listeners.ProcessorMemberChange;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Worker {
  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

  private final int workerId;
  private final ZkClient zkClient;
  private final String groupPath;
  private WorkerState state;
  private String ephemeralId;
  private String currentSubscription = null;
  private final LeaderChangeListener leaderChangeListener = new LeaderChangeListener();
  private final ProcessorMemberChange processorMemberChange = new ProcessorMemberChange();
  private final ZkSessionListener zkSessionListener = new ZkSessionListener();

  private final Random random = new Random();

  public Worker(int workerId, String groupPath, ZkClient zkClient) {
    this.workerId = workerId;
    this.zkClient = zkClient;
    this.groupPath = groupPath;
    state = WorkerState.INITIALIZED;
  }

  public void startUp() {
    if (state != WorkerState.INITIALIZED) {
      LOGGER.warn("Trying to start an already initialized worker!!");
    } else {
      zkClient.waitUntilConnected(5000, TimeUnit.MILLISECONDS);
      zkClient.subscribeStateChanges(zkSessionListener);

      try {
        String createdPath = zkClient.createEphemeralSequential(groupPath + "/" + String.valueOf("worker."), null);
        ephemeralId = createdPath.substring(createdPath.indexOf("worker"));
        LOGGER.info("Ephemeral ID for worker " + workerId + " is " + ephemeralId);

//      zkClient.createPersistent(String.format("%s%s/%s", groupPath, ZkUtils.PROCESSORS_PATH, workerId), workerId);
      } catch (Exception e)  {
        LOGGER.error(workerId + " - " + e);
        throw new RuntimeException(e);
      }
      state = WorkerState.RUNNING;
    }
  }

  public void joinElection() {
    if (state != WorkerState.RUNNING) {
      throw new RuntimeException("Worker is not running. Cannot join leader election!");
    } else {
      List<String> children = zkClient.getChildren(groupPath);
      assert children.size() > 0;
      Collections.sort(children);
      LOGGER.info("Found these children - " + children);

      int index = children.indexOf(ephemeralId);

      if (index == -1) {
        LOGGER.info("Looks like we lost connection with ZK!! Not handling reconnect for now");
        throw new RuntimeException("Looks like we lost connection with ZK!! Not handling reconnect for now");
      }

      if (index == 0) {
        LOGGER.info("Becoming Leader");
        zkClient.subscribeChildChanges(ZkUtils.PROCESSORS_PATH, processorMemberChange);
      } else {
        LOGGER.info("Not eligible for leader. Waiting for Leader to be elected");
        String prevCandidate = children.get(index - 1);
        if (!prevCandidate.equals(currentSubscription)) {
          if (currentSubscription != null) {
            zkClient.unsubscribeDataChanges(ZkUtils.PROCESSORS_PATH + "/" + currentSubscription, leaderChangeListener);
          }
          currentSubscription = prevCandidate;
          LOGGER.info("Subscribing to " +  currentSubscription);
          zkClient.subscribeDataChanges(ZkUtils.PROCESSORS_PATH + "/" + currentSubscription, leaderChangeListener);
        }

        // Double check that previous candidate exists
        boolean prevCandidateExists = zkClient.exists(ZkUtils.PROCESSORS_PATH + "/" + currentSubscription);
        if (prevCandidateExists) {
          LOGGER.info("Previous candidate still exists. Let's wait for it to away!");
        } else {
          try {
            Thread.sleep(random.nextInt(1000));
          } catch (InterruptedException e) {
            Thread.interrupted();
          }
          joinElection();
        }
      }

    }
  }


  public void stop() {
    zkClient.close();
  }

  class ZkSessionListener implements IZkStateListener {
    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
      LOGGER.info("State Changed Notification " + state.toString());
      if (state.equals(Watcher.Event.KeeperState.SyncConnected)) {
        LOGGER.info("KeeperState is syncConnected");
      } else {
        LOGGER.warn("Not sure about this state - " + state);
      }
    }

    @Override
    public void handleNewSession() throws Exception {
      // No-op for now
      LOGGER.info("New Session Callback");
    }

    @Override
    public void handleSessionEstablishmentError(Throwable error) throws Exception {
      // No-op for now
      LOGGER.info("Session Establishment Error - " + error.getMessage());
    }
  }

  class LeaderChangeListener implements IZkDataListener {
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      LOGGER.info("Data changed - " + dataPath +  " Changed object is " + data);
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      LOGGER.info("Previous worker was deleted - " + dataPath);
      joinElection();
    }
  }

}
