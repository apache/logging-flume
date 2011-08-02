package com.cloudera.flume.conf.avro;

@SuppressWarnings("all")
public class FlumeReportAvro extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"FlumeReportAvro\",\"namespace\":\"com.cloudera.flume.conf.avro\",\"fields\":[{\"name\":\"stringMetrics\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"longMetrics\",\"type\":{\"type\":\"map\",\"values\":\"long\"}},{\"name\":\"doubleMetrics\",\"type\":{\"type\":\"map\",\"values\":\"double\"}}]}");
  public java.util.Map<org.apache.avro.util.Utf8,org.apache.avro.util.Utf8> stringMetrics;
  public java.util.Map<org.apache.avro.util.Utf8,java.lang.Long> longMetrics;
  public java.util.Map<org.apache.avro.util.Utf8,java.lang.Double> doubleMetrics;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return stringMetrics;
    case 1: return longMetrics;
    case 2: return doubleMetrics;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: stringMetrics = (java.util.Map<org.apache.avro.util.Utf8,org.apache.avro.util.Utf8>)value$; break;
    case 1: longMetrics = (java.util.Map<org.apache.avro.util.Utf8,java.lang.Long>)value$; break;
    case 2: doubleMetrics = (java.util.Map<org.apache.avro.util.Utf8,java.lang.Double>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
