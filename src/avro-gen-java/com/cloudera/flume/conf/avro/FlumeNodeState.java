package com.cloudera.flume.conf.avro;

@SuppressWarnings("all")
public enum FlumeNodeState { 
  HELLO, IDLE, CONFIGURING, ACTIVE, ERROR, LOST, DECOMMISSIONED
}
