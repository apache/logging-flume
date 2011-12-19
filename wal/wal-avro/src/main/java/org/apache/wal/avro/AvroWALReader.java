package org.apache.wal.avro;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.wal.WALEntry;
import org.apache.wal.WALReader;
import org.apache.wal.avro.AvroWALEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvroWALReader implements WALReader {

  private static final Logger logger = LoggerFactory
      .getLogger(AvroWALReader.class);

  private FileInputStream walInputStream;
  private MappedByteBuffer indexBuffer;
  private Decoder decoder;
  private SpecificDatumReader<AvroWALEntry> avroReader;

  private long currentPosition;
  private FileChannel outputChannel;

  public void open() {
    outputChannel = walInputStream.getChannel();

    try {
      outputChannel.position(indexBuffer.getLong(0));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public WALEntry next() {
    WALEntry entry = null;

    try {
      entry = new AvroWALEntryAdapter(avroReader.read(null, decoder));
      currentPosition = walInputStream.getChannel().position();

      if (logger.isDebugEnabled()) {
        logger.debug("Wrote entry:{} markPosition:{} currentPosition:{}",
            new Object[] { entry, indexBuffer.getLong(0), currentPosition });
      }
    } catch (IOException e) {
      logger.error("Failed to read WAL entry. Exception follows.", e);
    }

    return entry;
  }

  @Override
  public void mark() {
    logger.debug("Updating currentPosition to:{}", currentPosition);

    indexBuffer.putLong(0, currentPosition);
    indexBuffer.force();
  }

  @Override
  public void reset() {
    logger.debug("Rewinding last successful read position");

    try {
      walInputStream.getChannel().position(indexBuffer.getLong(0));
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

  public FileInputStream getWalInputStream() {
    return walInputStream;
  }

  public void setWalInputStream(FileInputStream walInputStream) {
    this.walInputStream = walInputStream;
  }

  public MappedByteBuffer getIndexBuffer() {
    return indexBuffer;
  }

  public void setIndexBuffer(MappedByteBuffer indexBuffer) {
    this.indexBuffer = indexBuffer;
  }

  public Decoder getDecoder() {
    return decoder;
  }

  public void setDecoder(Decoder decoder) {
    this.decoder = decoder;
  }

  public SpecificDatumReader<AvroWALEntry> getAvroReader() {
    return avroReader;
  }

  public void setAvroReader(SpecificDatumReader<AvroWALEntry> avroReader) {
    this.avroReader = avroReader;
  }

}
