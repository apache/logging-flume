package org.apache.wal.avro;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.wal.WALEntry;
import org.apache.wal.WALWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class AvroWALWriter implements WALWriter {

  private static final Logger logger = LoggerFactory
      .getLogger(AvroWALWriter.class);

  private FileOutputStream walOutputStream;
  private MappedByteBuffer indexBuffer;
  private Encoder encoder;
  private SpecificDatumWriter<AvroWALEntry> writer;

  private long currentPosition;
  private FileChannel outputChannel;

  @Override
  public void open() {
    outputChannel = walOutputStream.getChannel();

    try {
      indexBuffer.putLong(0, outputChannel.position());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    indexBuffer.force();
  }

  @Override
  public void write(WALEntry entry) {
    Preconditions.checkArgument(entry instanceof AvroWALEntryAdapter,
        "WAL entry must be an instance of AvroWALEntryAdapter");

    try {
      writer.write(((AvroWALEntryAdapter) entry).getEntry(), encoder);
      encoder.flush();
      outputChannel.force(false);
      currentPosition = outputChannel.position();

      if (logger.isDebugEnabled()) {
        logger.debug("Wrote entry:{} markPosition:{} currentPosition:{}",
            new Object[] { entry, indexBuffer.getLong(0), currentPosition });
      }
    } catch (IOException e) {
      logger.error("Failed to write WAL entry. Exception follows.", e);
    }
  }

  @Override
  public void close() {
  }

  @Override
  public void mark() {
    indexBuffer.putLong(0, currentPosition);
    indexBuffer.force();
  }

  @Override
  public void reset() {
    try {
      outputChannel.truncate(indexBuffer.getLong(0));
      /*
       * Changes to the file size affect the metadata so we need to force that
       * out as well and pay the price of the second IO.
       */
      outputChannel.force(true);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public long getCurrentPosition() {
    return currentPosition;
  }

  public long getMarkPosition() {
    return indexBuffer.getLong(0);
  }

  public FileOutputStream getWalOutputStream() {
    return walOutputStream;
  }

  public void setWalOutputStream(FileOutputStream walOutputStream) {
    this.walOutputStream = walOutputStream;
  }

  public MappedByteBuffer getIndexBuffer() {
    return indexBuffer;
  }

  public void setIndexBuffer(MappedByteBuffer indexBuffer) {
    this.indexBuffer = indexBuffer;
  }

  public Encoder getEncoder() {
    return encoder;
  }

  public void setEncoder(Encoder encoder) {
    this.encoder = encoder;
  }

  public SpecificDatumWriter<AvroWALEntry> getWriter() {
    return writer;
  }

  public void setWriter(SpecificDatumWriter<AvroWALEntry> writer) {
    this.writer = writer;
  }

}
