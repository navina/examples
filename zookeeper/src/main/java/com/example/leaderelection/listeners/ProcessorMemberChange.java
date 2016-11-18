package com.example.leaderelection.listeners;

import org.I0Itec.zkclient.IZkChildListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProcessorMemberChange implements IZkChildListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorMemberChange.class);

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
    LOGGER.info("Children Changed Callback!");
    for (String child:  currentChilds) {
      LOGGER.info("Child - " +  child);
    }
  }
}
