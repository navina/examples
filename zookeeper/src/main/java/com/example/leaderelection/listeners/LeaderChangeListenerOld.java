package com.example.leaderelection.listeners;

import org.I0Itec.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderChangeListenerOld implements IZkDataListener {
  public static final Logger LOGGER = LoggerFactory.getLogger(LeaderChangeListenerOld.class);
  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception {
    LOGGER.info("Leader Data changed - " + dataPath +  " Changed object is " + data);
  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception {
    LOGGER.info("Leader Path for Deleted! " + dataPath);

  }
}
