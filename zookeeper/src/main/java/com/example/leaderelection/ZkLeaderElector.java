package com.example.leaderelection;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class ZkLeaderElector implements LeaderElection {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkLeaderElector.class);
  private final ZkUtils zkUtils;

  // State in LeaderElector
  private String currentEphemeralPath = null;
  private String leaderId = null;
  private String currentSubscription = null;
  private final Random random = new Random();

  private final ZkControllerListener listener;
  private final ZkLeaderListener leaderListener = new ZkLeaderListener();
  private final ZkWorkerChangeListener workerChangeListener= new ZkWorkerChangeListener();
  private final ZkController.TaskAssignmentHandler taskAssignmentHandler;

  public ZkLeaderElector (
      ZkUtils zkUtils,
      ZkControllerListener listener,
      ZkController.TaskAssignmentHandler taskAssignmentHandler) {
    this.zkUtils = zkUtils;
    this.listener = listener;
    this.taskAssignmentHandler = taskAssignmentHandler;
  }

  @Override
  public void tryBecomeLeader() {
    if (currentEphemeralPath == null) {
      currentEphemeralPath = zkUtils.registerAsWorker();
//      zkUtils.subscribeToTaskAssignmentChange(currentEphemeralPath, taskAssignmentHandler);
      zkUtils.subscribeToTaskStatus(taskAssignmentHandler);
    }
    List<String> children = zkUtils.findActiveWorkers();
    int index = children.indexOf(ZkUtils.parseWorkerIdFromPath(currentEphemeralPath));

    if (index == -1) {
      LOGGER.info("Looks like we lost connection with ZK!! Not handling reconnect for now");
      throw new RuntimeException("Looks like we lost connection with ZK!! Not handling reconnect for now");
    }

    if (index == 0) {
      LOGGER.info("Becoming Leader");
      leaderId = ZkUtils.parseWorkerIdFromPath(currentEphemeralPath);
      zkUtils.subscribeToWorkerChange(workerChangeListener);
      listener.onBecomeLeader();
    } else {
      LOGGER.info("Not eligible for leader. Waiting for Leader to be elected");
      String prevCandidate = children.get(index - 1);
      if (!prevCandidate.equals(currentSubscription)) {
        if (currentSubscription != null) {
          zkUtils.unsubscribeDataChanges(zkUtils.getProcessorsPath() + "/" + currentSubscription, leaderListener);
        }
        currentSubscription = prevCandidate;
        LOGGER.info("Subscribing to " + currentSubscription);
        zkUtils.subscribeDataChanges(zkUtils.getProcessorsPath() + "/" + currentSubscription, leaderListener);
      }

      // Double check that previous candidate exists
      boolean prevCandidateExists = zkUtils.exists(zkUtils.getProcessorsPath() + "/" + currentSubscription);
      if (prevCandidateExists) {
        LOGGER.info("Previous candidate still exists. Let's wait for it to away!");
      } else {
        try {
          Thread.sleep(random.nextInt(1000));
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
        tryBecomeLeader();
      }
    }
  }

  @Override
  public void resignLeadership() {
    zkUtils.unsubscribeToWorkerChange(workerChangeListener);
    leaderId = null;
  }

  @Override
  public boolean amILeader() {
    return currentEphemeralPath != null &&
        leaderId != null &&
        leaderId.equals(ZkUtils.parseWorkerIdFromPath(currentEphemeralPath));
  }

  // TODO: Figure out how to get rid of this accessor
  // Can return null
  @Override
  public String getMyCurrentId() {
    return currentEphemeralPath;
  }

  // All non-leaders
  class ZkLeaderListener implements IZkDataListener {

    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      LOGGER.info("ZkLeaderElector::ZkLeaderChangeListener::handleDataChange - Data for path " + dataPath + " changed to " + data);
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      LOGGER.info("ZkLeaderElector::ZkLeaderChangeListener::handleDataDeleted - Data for path " + dataPath);
      tryBecomeLeader();
    }
  }

  // All Leaders
  class ZkWorkerChangeListener implements IZkChildListener {

    /**
     * Called when the children of the given path changed.
     *
     * @param parentPath    The parent path
     * @param currentChilds The children or null if the root node (parent path) was deleted.
     * @throws Exception
     */
    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
      LOGGER.info("ZkLeaderElector::ZkWorkerChangeListener::handleChildChange - Current Children : " + currentChilds);
      listener.onWorkerChange(currentChilds);
    }
  }

}
