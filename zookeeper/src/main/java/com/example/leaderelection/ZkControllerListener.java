package com.example.leaderelection;

import java.util.List;

public interface ZkControllerListener {
  void onBecomeLeader();
  void onWorkerChange(List<String> currentWorkers);
//  void onTaskAssignmentChange(List<Task> tasks);
}
