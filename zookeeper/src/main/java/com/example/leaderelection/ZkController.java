package com.example.leaderelection;


import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class ZkController implements ZkControllerListener, IZkStateListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkController.class);

  private final LeaderElection leaderElection;
  private final ZkUtils zkUtils;
  private final TaskController taskController;
  private final int workerId;
  private final TaskAssignmentHandler taskAssignmentHandler = new TaskAssignmentHandler();
  private final ScheduledExecutorService schedExecutor = Executors.newSingleThreadScheduledExecutor();

  public ZkController (int workerId, ZkUtils zkUtils, TaskController taskController) {
    this.workerId = workerId; // Used for logging and understanding only
    this.zkUtils = zkUtils;
    this.leaderElection = new ZkLeaderElector(this.zkUtils, this, taskAssignmentHandler);
    this.taskController = taskController;
  }

  public void start() {
    zkUtils.getZkClient().subscribeStateChanges(this);
    leaderElection.tryBecomeLeader();
  }

  public void stop() {
    if(leaderElection.amILeader()) {
      leaderElection.resignLeadership();
    }
    zkUtils.getZkClient().unsubscribeStateChanges(this);  // is this needed?
    zkUtils.close();
  }

  @Override
  public void onBecomeLeader() {
    LOGGER.info("ZkController::ZkControllerListener::onBecomeLeader - Became Leader. Doing leader like things");
    zkUtils.lock.lock();
    zkUtils.setTaskStatus("STOP");
    zkUtils.lock.unlock();
    scheduleTaskAssignment();
  }

  private void scheduleTaskAssignment() {
    schedExecutor.schedule(
        new Runnable() {
          @Override
          public void run() {
            List<String> workers = zkUtils.findActiveWorkers();
            int perWorkerTask = 10 / workers.size();
            if (perWorkerTask < 1)
              perWorkerTask = 1;

            int runningCounter = 1;
            for (int w = 0; w < workers.size() && runningCounter <= 10; w++) {
              String worker = workers.get(w);
              String writeData = "";
              for (int i = 0; i < perWorkerTask; i++) {
                writeData += runningCounter;
                writeData += ",";
                runningCounter++;
              }
              LOGGER.info("Assigning Tasks after Scheduled Delay!!");
              zkUtils.assignTasks(worker, writeData.substring(0, writeData.lastIndexOf(",")));
            }
            // Set status to start
            zkUtils.lock.lock();
            LOGGER.info("Setting the status flag to START");
            zkUtils.setTaskStatus("START");
            zkUtils.lock.unlock();
          }
        },
        10000,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void onWorkerChange(List<String> currentWorkers) {
    LOGGER.info("ZkLeaderElector::ZkWorkerChangeListener::handleChildChange - Current Children : " + currentWorkers);
    LOGGER.info("Stopping my tasks!");
    taskController.stop();
    if (leaderElection.amILeader()) {
      LOGGER.info("I am the leader. So, I am calling taskAssignment");
      zkUtils.lock.lock();
      zkUtils.setTaskStatus("STOP");
      zkUtils.lock.unlock();
      scheduleTaskAssignment();
    }
  }

//  @Override
/*  public void onTaskAssignmentChange(List<Task> tasks) {
    LOGGER.info("ZkController::ZkControllerListener:onTaskAssignmentChange - Current Tasks: " + tasks);

  }*/

  // All Workers -> /group/assign/worker-$id
  // Now -> /group/status
  class TaskAssignmentHandler implements IZkDataListener {
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      String status = (String) data;
      if (status != null && !status.isEmpty()) {
        switch (status) {
          case "START":
            LOGGER.info("==========================================================================================");
            LOGGER.info("TaskAssignmentHandler::handleDataChange:: Task Status set to START. Fetching new tasklist");
            String currentPath = leaderElection.getMyCurrentId();
            if (currentPath == null) {
              LOGGER.error("Something went wrong. I don't have an ephemeral path!!");
            }
            String assignedTaskString = zkUtils.getAssignedTasks(currentPath);
            if (assignedTaskString == null || assignedTaskString.isEmpty()) {
              LOGGER.info("Assigned Task String is null or empty. Not doing anything.");
            } else {
              String[] multipliers = assignedTaskString.split(",");
              List<Task> tasks = new ArrayList<>();
              for (int i=0; i < multipliers.length; i++) {
                tasks.add(new Task(workerId, Integer.parseInt(multipliers[i])));
              }
              LOGGER.info("Starting with Tasks - " + tasks);
              taskController.start(tasks);
            }
            LOGGER.info("==========================================================================================");
            break;
          case "STOP":
            LOGGER.info("==========================================================================================");
            LOGGER.info("TaskAssignmentHandler::handleDataChange:: Task Status set to STOP. Hence, stopping all tasks");
            taskController.stop();
            LOGGER.info("==========================================================================================");
            break;
          default:
            LOGGER.info("Not doing anything. Unknown status - " + data);
            break;
        }
      }
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {

    }
  }
  /**
   * Called when the zookeeper connection state has changed.
   *
   * @param state The new state.
   * @throws Exception On any error.
   */
  @Override
  public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {

  }

  /**
   * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
   * any ephemeral nodes here.
   *
   * @throws Exception On any error.
   */
  @Override
  public void handleNewSession() throws Exception {
    if (leaderElection.amILeader()) {
      leaderElection.resignLeadership();
    }
    leaderElection.tryBecomeLeader();
  }

  /**
   * Called when a session cannot be re-established. This should be used to implement connection
   * failure handling e.g. retry to connect or pass the error up
   *
   * @param error The error that prevents a session from being established
   * @throws Exception On any error.
   */
  @Override
  public void handleSessionEstablishmentError(Throwable error) throws Exception {

  }
}
