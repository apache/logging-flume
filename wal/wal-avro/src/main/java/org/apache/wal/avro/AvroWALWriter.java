package org.apache.wal.avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.wal.WALEntry;
import org.apache.wal.WALWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.io.Files;

public class AvroWALWriter implements WALWriter {

  private static final short writeIndexVersion = 1;
  private static final Logger logger = LoggerFactory
      .getLogger(AvroWALWriter.class);

  private File directory;

  private FileOutputStream walOutputStream;
  private MappedByteBuffer indexBuffer;
  private Encoder encoder;
  private SpecificDatumWriter<AvroWALEntry> writer;

  private ByteArrayOutputStream indexOutputStream;
  private Encoder indexEncoder;
  private SpecificDatumWriter<AvroWALIndex> indexWriter;

  private File currentFile;
  private long currentPosition;
  private FileChannel outputChannel;

  @Override
  public void open() {
    logger.info("Opening write ahead log in:{}", directory);

    currentFile = new File(directory, System.currentTimeMillis() + ".wal");

    try {
      walOutputStream = new FileOutputStream(currentFile, true);
      outputChannel = walOutputStream.getChannel();
      currentPosition = outputChannel.position();

      indexOutputStream = new ByteArrayOutputStream(1024);
      indexEncoder = EncoderFactory.get().jsonEncoder(AvroWALIndex.SCHEMA$,
          indexOutputStream);
      indexWriter = new SpecificDatumWriter<AvroWALIndex>(AvroWALIndex.class);

      indexBuffer = Files.map(new File(directory, "write.idx"),
          FileChannel.MapMode.READ_WRITE, 8 * 1024);

      updateWriteIndex(currentPosition, currentFile.getPath());
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    encoder = EncoderFactory.get().directBinaryEncoder(walOutputStream, null);
    writer = new SpecificDatumWriter<AvroWALEntry>(AvroWALEntry.class);

    logger.debug("Opened write ahead log:{} currentPosition:{}", currentFile,
        currentPosition);
  }

  private void updateWriteIndex(long currentPosition, String path) {
    logger.debug("Updating write index to position:{} path:{}",
        currentPosition, path);

    AvroWALIndex index = AvroWALIndex
        .newBuilder()
        .setVersion(writeIndexVersion)
        .setEntries(
            Arrays.asList(AvroWALIndexEntry.newBuilder()
                .setPath(currentFile.getPath()).setPosition(currentPosition)
                .build())).build();

    indexOutputStream.reset();

    try {
      indexWriter.write(index, indexEncoder);
      indexEncoder.flush();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    indexBuffer.put(indexOutputStream.toByteArray());
    indexBuffer.force();
    indexBuffer.flip();
  }

  @Override
  public void write(WALEntry entry) {
    Preconditions.checkArgument(entry instanceof AvroWALEntryAdapter,
        "WAL entry must be an instance of AvroWALEntryAdapter");

    try {
      writer.write(((AvroWALEntryAdapter) entry).getEntry(), encoder);
      encoder.flush();
      outputChannel.force(true);
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
    logger.info("Closing write ahead log at:{}", currentFile);

    Closeables.closeQuietly(outputChannel);
  }

  @Override
  public void mark() {
    updateWriteIndex(currentPosition, currentFile.getPath());
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
