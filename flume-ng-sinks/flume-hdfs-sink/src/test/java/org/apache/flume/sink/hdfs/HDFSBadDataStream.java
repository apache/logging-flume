package org.apache.flume.sink.hdfs;

import java.io.IOException;

import org.apache.flume.Event;
import org.apache.flume.sink.FlumeFormatter;
import org.apache.flume.sink.hdfs.HDFSSequenceFile;


public class HDFSBadDataStream extends HDFSDataStream {
  public class HDFSBadSeqWriter extends HDFSSequenceFile {
    @Override
    public void append(Event e, FlumeFormatter fmt) throws IOException {

      if (e.getHeaders().containsKey("fault")) {
        throw new IOException("Injected fault");
      }
      super.append(e, fmt);
    }

  }

}
