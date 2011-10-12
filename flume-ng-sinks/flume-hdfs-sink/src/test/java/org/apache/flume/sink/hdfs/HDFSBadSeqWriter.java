package org.apache.flume.sink.hdfs;


import java.io.IOException;

import org.apache.flume.Event;
import org.apache.flume.sink.FlumeFormatter;

public class HDFSBadSeqWriter extends HDFSSequenceFile {
  @Override
  public void append(Event e, FlumeFormatter fmt) throws IOException {

    if (e.getHeaders().containsKey("fault")) {
      throw new IOException("Injected fault");
    } else if (e.getHeaders().containsKey("fault-once")) {
      e.getHeaders().remove("fault-once");
      throw new IOException("Injected fault");
    }
    super.append(e, fmt);
  }

}
