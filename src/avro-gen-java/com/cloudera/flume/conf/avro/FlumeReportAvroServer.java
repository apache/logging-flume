package com.cloudera.flume.conf.avro;

@SuppressWarnings("all")
public interface FlumeReportAvroServer {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"FlumeReportAvroServer\",\"namespace\":\"com.cloudera.flume.conf.avro\",\"types\":[{\"type\":\"enum\",\"name\":\"FlumeNodeState\",\"symbols\":[\"HELLO\",\"IDLE\",\"CONFIGURING\",\"ACTIVE\",\"ERROR\",\"LOST\",\"DECOMMISSIONED\"]},{\"type\":\"record\",\"name\":\"AvroFlumeConfigData\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"sourceConfig\",\"type\":\"string\"},{\"name\":\"sinkConfig\",\"type\":\"string\"},{\"name\":\"sourceVersion\",\"type\":\"long\"},{\"name\":\"sinkVersion\",\"type\":\"long\"},{\"name\":\"flowID\",\"type\":\"string\"}]},{\"type\":\"record\",\"name\":\"FlumeReportAvro\",\"fields\":[{\"name\":\"stringMetrics\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"longMetrics\",\"type\":{\"type\":\"map\",\"values\":\"long\"}},{\"name\":\"doubleMetrics\",\"type\":{\"type\":\"map\",\"values\":\"double\"}}]}],\"messages\":{\"heartbeat\":{\"request\":[{\"name\":\"logicalNode\",\"type\":\"string\"},{\"name\":\"physicalNode\",\"type\":\"string\"},{\"name\":\"host\",\"type\":\"string\"},{\"name\":\"s\",\"type\":\"FlumeNodeState\"},{\"name\":\"timestamp\",\"type\":\"long\"}],\"response\":\"boolean\"},\"getConfig\":{\"request\":[{\"name\":\"physNode\",\"type\":\"string\"}],\"response\":[\"AvroFlumeConfigData\",\"null\"]},\"getLogicalNodes\":{\"request\":[{\"name\":\"physNode\",\"type\":\"string\"}],\"response\":{\"type\":\"array\",\"items\":\"string\"}},\"acknowledge\":{\"request\":[{\"name\":\"ackid\",\"type\":\"string\"}],\"response\":\"null\"},\"checkAck\":{\"request\":[{\"name\":\"ackid\",\"type\":\"string\"}],\"response\":\"boolean\"},\"putReports\":{\"request\":[{\"name\":\"reports\",\"type\":{\"type\":\"map\",\"values\":\"FlumeReportAvro\"}}],\"response\":\"null\"}}}");
  boolean heartbeat(org.apache.avro.util.Utf8 logicalNode, org.apache.avro.util.Utf8 physicalNode, org.apache.avro.util.Utf8 host, com.cloudera.flume.conf.avro.FlumeNodeState s, long timestamp)
    throws org.apache.avro.ipc.AvroRemoteException;
  com.cloudera.flume.conf.avro.AvroFlumeConfigData getConfig(org.apache.avro.util.Utf8 physNode)
    throws org.apache.avro.ipc.AvroRemoteException;
  org.apache.avro.generic.GenericArray<org.apache.avro.util.Utf8> getLogicalNodes(org.apache.avro.util.Utf8 physNode)
    throws org.apache.avro.ipc.AvroRemoteException;
  java.lang.Void acknowledge(org.apache.avro.util.Utf8 ackid)
    throws org.apache.avro.ipc.AvroRemoteException;
  boolean checkAck(org.apache.avro.util.Utf8 ackid)
    throws org.apache.avro.ipc.AvroRemoteException;
  java.lang.Void putReports(java.util.Map<org.apache.avro.util.Utf8,com.cloudera.flume.conf.avro.FlumeReportAvro> reports)
    throws org.apache.avro.ipc.AvroRemoteException;
}
