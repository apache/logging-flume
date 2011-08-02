package com.cloudera.flume.conf.avro;

@SuppressWarnings("all")
public class FlumeNodeStatusAvro extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"FlumeNodeStatusAvro\",\"namespace\":\"com.cloudera.flume.conf.avro\",\"fields\":[{\"name\":\"state\",\"type\":{\"type\":\"enum\",\"name\":\"FlumeNodeState\",\"symbols\":[\"HELLO\",\"IDLE\",\"CONFIGURING\",\"ACTIVE\",\"ERROR\",\"LOST\",\"DECOMMISSIONED\"]}},{\"name\":\"version\",\"type\":\"long\"},{\"name\":\"lastseen\",\"type\":\"long\"},{\"name\":\"lastSeenDeltaMillis\",\"type\":\"long\"},{\"name\":\"host\",\"type\":\"string\"},{\"name\":\"physicalNode\",\"type\":\"string\"}]}");
  public com.cloudera.flume.conf.avro.FlumeNodeState state;
  public long version;
  public long lastseen;
  public long lastSeenDeltaMillis;
  public org.apache.avro.util.Utf8 host;
  public org.apache.avro.util.Utf8 physicalNode;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return state;
    case 1: return version;
    case 2: return lastseen;
    case 3: return lastSeenDeltaMillis;
    case 4: return host;
    case 5: return physicalNode;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: state = (com.cloudera.flume.conf.avro.FlumeNodeState)value$; break;
    case 1: version = (java.lang.Long)value$; break;
    case 2: lastseen = (java.lang.Long)value$; break;
    case 3: lastSeenDeltaMillis = (java.lang.Long)value$; break;
    case 4: host = (org.apache.avro.util.Utf8)value$; break;
    case 5: physicalNode = (org.apache.avro.util.Utf8)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
