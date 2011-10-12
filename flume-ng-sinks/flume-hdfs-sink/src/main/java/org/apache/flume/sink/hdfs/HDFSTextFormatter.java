package org.apache.flume.sink.hdfs;

import org.apache.flume.Event;
import org.apache.flume.sink.FlumeFormatter;
//import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;


public class HDFSTextFormatter implements FlumeFormatter {

  private Text makeText(Event e) {
    Text textObject = new Text();
    textObject.set(e.getBody(), 0, e.getBody().length);
    return textObject;
  }

  @Override
  public Class<LongWritable> getKeyClass() {
    return LongWritable.class;
  }

  @Override
  public Class<Text> getValueClass() {
    return Text.class;
  }

  @Override
  public Object getKey(Event e) {
    // Write the data to HDFS
    String timestamp = e.getHeaders().get("timestamp");
    long eventStamp;

    if (timestamp == null) {
      eventStamp = System.currentTimeMillis();
    } else {
      eventStamp = Long.valueOf(timestamp);
    }
    LongWritable longObject = new LongWritable(eventStamp);
    return longObject;
  }

  @Override
  public Object getValue(Event e) {
    return makeText(e);
  }

  @Override
  public byte[] getBytes(Event e) {
    Text record = makeText(e);
    record.append("\n".getBytes(), 0, 1);
    return record.getBytes();
  }

}
