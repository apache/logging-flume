package com.cloudera.flume.conf.avro;

@SuppressWarnings("all")
public class AvroFlumeConfigData extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"AvroFlumeConfigData\",\"namespace\":\"com.cloudera.flume.conf.avro\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"sourceConfig\",\"type\":\"string\"},{\"name\":\"sinkConfig\",\"type\":\"string\"},{\"name\":\"sourceVersion\",\"type\":\"long\"},{\"name\":\"sinkVersion\",\"type\":\"long\"},{\"name\":\"flowID\",\"type\":\"string\"}]}");
  public long timestamp;
  public org.apache.avro.util.Utf8 sourceConfig;
  public org.apache.avro.util.Utf8 sinkConfig;
  public long sourceVersion;
  public long sinkVersion;
  public org.apache.avro.util.Utf8 flowID;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return timestamp;
    case 1: return sourceConfig;
    case 2: return sinkConfig;
    case 3: return sourceVersion;
    case 4: return sinkVersion;
    case 5: return flowID;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: timestamp = (java.lang.Long)value$; break;
    case 1: sourceConfig = (org.apache.avro.util.Utf8)value$; break;
    case 2: sinkConfig = (org.apache.avro.util.Utf8)value$; break;
    case 3: sourceVersion = (java.lang.Long)value$; break;
    case 4: sinkVersion = (java.lang.Long)value$; break;
    case 5: flowID = (org.apache.avro.util.Utf8)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
