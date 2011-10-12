package org.apache.flume.sink.hdfs;

import java.io.IOException;

import org.apache.flume.sink.hdfs.HDFSBadSeqWriter;
import org.apache.flume.sink.hdfs.HDFSBadDataStream;

public class HDFSBadWriterFactory extends HDFSWriterFactory {
  static final String BadSequenceFileType = "SequenceFile";
  static final String BadDataStreamType = "DataStream";
  static final String BadCompStreamType = "CompressedStream";

  public HDFSWriter getWriter(String fileType) throws IOException {
    if (fileType == BadSequenceFileType) {
      return new HDFSBadSeqWriter();
    } else if (fileType == BadDataStreamType) {
      return new HDFSBadDataStream();
    } else {
      throw new IOException("File type " + fileType + " not supported");
    }
  }
}
