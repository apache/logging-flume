package com.cloudera.flume.conf.avro;

@SuppressWarnings("all")
public class FlumeMasterCommandAvro extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"FlumeMasterCommandAvro\",\"namespace\":\"com.cloudera.flume.conf.avro\",\"fields\":[{\"name\":\"command\",\"type\":\"string\"},{\"name\":\"arguments\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}");
  public org.apache.avro.util.Utf8 command;
  public org.apache.avro.generic.GenericArray<org.apache.avro.util.Utf8> arguments;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return command;
    case 1: return arguments;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: command = (org.apache.avro.util.Utf8)value$; break;
    case 1: arguments = (org.apache.avro.generic.GenericArray<org.apache.avro.util.Utf8>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
