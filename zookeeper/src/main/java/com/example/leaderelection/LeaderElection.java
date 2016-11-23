package com.example.leaderelection;

public interface LeaderElection {
  void tryBecomeLeader();
  void resignLeadership();
  boolean amILeader();
  // TODO: Try to move this out of the interface
  String getMyCurrentId();
}
