package com.cloudera.flume.conf.avro;

@SuppressWarnings("all")
public interface FlumeMasterAdminServerAvro {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"FlumeMasterAdminServerAvro\",\"namespace\":\"com.cloudera.flume.conf.avro\",\"types\":[{\"type\":\"enum\",\"name\":\"FlumeNodeState\",\"symbols\":[\"HELLO\",\"IDLE\",\"CONFIGURING\",\"ACTIVE\",\"ERROR\",\"LOST\",\"DECOMMISSIONED\"]},{\"type\":\"record\",\"name\":\"AvroFlumeConfigData\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"sourceConfig\",\"type\":\"string\"},{\"name\":\"sinkConfig\",\"type\":\"string\"},{\"name\":\"sourceVersion\",\"type\":\"long\"},{\"name\":\"sinkVersion\",\"type\":\"long\"},{\"name\":\"flowID\",\"type\":\"string\"}]},{\"type\":\"record\",\"name\":\"FlumeMasterCommandAvro\",\"fields\":[{\"name\":\"command\",\"type\":\"string\"},{\"name\":\"arguments\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]},{\"type\":\"record\",\"name\":\"FlumeNodeStatusAvro\",\"fields\":[{\"name\":\"state\",\"type\":\"FlumeNodeState\"},{\"name\":\"version\",\"type\":\"long\"},{\"name\":\"lastseen\",\"type\":\"long\"},{\"name\":\"lastSeenDeltaMillis\",\"type\":\"long\"},{\"name\":\"host\",\"type\":\"string\"},{\"name\":\"physicalNode\",\"type\":\"string\"}]}],\"messages\":{\"submit\":{\"request\":[{\"name\":\"command\",\"type\":\"FlumeMasterCommandAvro\"}],\"response\":\"long\"},\"isSuccess\":{\"request\":[{\"name\":\"cmdid\",\"type\":\"long\"}],\"response\":\"boolean\"},\"isFailure\":{\"request\":[{\"name\":\"cmdid\",\"type\":\"long\"}],\"response\":\"boolean\"},\"getNodeStatuses\":{\"request\":[],\"response\":{\"type\":\"map\",\"values\":\"FlumeNodeStatusAvro\"}},\"getConfigs\":{\"request\":[],\"response\":{\"type\":\"map\",\"values\":\"AvroFlumeConfigData\"}},\"getMappings\":{\"request\":[{\"name\":\"physicalNode\",\"type\":\"string\"}],\"response\":{\"type\":\"map\",\"values\":{\"type\":\"array\",\"items\":\"string\"}}},\"hasCmdId\":{\"request\":[{\"name\":\"cmdid\",\"type\":\"long\"}],\"response\":\"boolean\"}}}");
  long submit(com.cloudera.flume.conf.avro.FlumeMasterCommandAvro command)
    throws org.apache.avro.ipc.AvroRemoteException;
  boolean isSuccess(long cmdid)
    throws org.apache.avro.ipc.AvroRemoteException;
  boolean isFailure(long cmdid)
    throws org.apache.avro.ipc.AvroRemoteException;
  java.util.Map<org.apache.avro.util.Utf8,com.cloudera.flume.conf.avro.FlumeNodeStatusAvro> getNodeStatuses()
    throws org.apache.avro.ipc.AvroRemoteException;
  java.util.Map<org.apache.avro.util.Utf8,com.cloudera.flume.conf.avro.AvroFlumeConfigData> getConfigs()
    throws org.apache.avro.ipc.AvroRemoteException;
  java.util.Map<org.apache.avro.util.Utf8,org.apache.avro.generic.GenericArray<org.apache.avro.util.Utf8>> getMappings(org.apache.avro.util.Utf8 physicalNode)
    throws org.apache.avro.ipc.AvroRemoteException;
  boolean hasCmdId(long cmdid)
    throws org.apache.avro.ipc.AvroRemoteException;
}
