package org.apache.flume.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reporter {

  private static final Logger logger = LoggerFactory.getLogger(Reporter.class);

  private long timeStamp;

  public Reporter() {
    timeStamp = System.currentTimeMillis();
  }

  public synchronized long progress() {
    long previousTimeStamp = timeStamp;
    timeStamp = System.currentTimeMillis();

    logger.debug("progress updated. previousTimeStamp:{} timeStamp:{}",
        previousTimeStamp, timeStamp);

    return timeStamp;
  }

  public synchronized long getTimeStamp() {
    return timeStamp;
  }

  @Override
  public String toString() {
    return "{ timeStamp:" + timeStamp + " }";
  }

}
