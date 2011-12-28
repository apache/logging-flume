package org.apache.wal.avro;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.wal.WALEntry;
import org.apache.wal.WALException;
import org.apache.wal.WALReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.io.Closeables;

public class AvroWALReader implements WALReader {

  private static final Logger logger = LoggerFactory
      .getLogger(AvroWALReader.class);

  private File directory;

  private FileInputStream walInputStream;
  private FileChannel inputChannel;
  private Decoder decoder;
  private SpecificDatumReader<AvroWALEntry> reader;

  private WALIndex index;

  private File currentFile;
  private long currentPosition;

  public AvroWALReader() {
    reader = new SpecificDatumReader<AvroWALEntry>(AvroWALEntry.class);
  }

  @Override
  public void open() {
    logger.info("Opening write ahead log reader for directory:{}", directory);

    if (index.getReadFile() != null) {
      openWALFile(new File(index.getReadFile()), index.getReadPosition());
    } else {
      File file = findNextFile();

      if (file != null) {
        openWALFile(file, 0);
      }
    }

    logger.debug("Opened write ahead log reader:{}", this);
  }

  @Override
  public void close() {
    closeWALFile();
  }

  private boolean ensureWALFile() {
    if (decoder == null) {
      File file = findNextFile();

      if (file != null) {
        openWALFile(file, 0);
      }
    }

    return decoder != null;
  }

  private void openWALFile(File file, long position) {
    currentFile = file;

    try {
      walInputStream = new FileInputStream(currentFile);
      inputChannel = walInputStream.getChannel();
      decoder = DecoderFactory.get().directBinaryDecoder(walInputStream, null);

      inputChannel.position(position);
    } catch (FileNotFoundException e) {
      throw new WALException("Unable to open write ahead log file:" + file
          + " position:" + position, e);
    } catch (IOException e) {
      throw new WALException("Unable to open write ahead log file:" + file
          + " position:" + position, e);
    }

    index.updateReadIndex(currentFile.getPath(), position);
  }

  private void closeWALFile() {
    Closeables.closeQuietly(walInputStream);
    Closeables.closeQuietly(inputChannel);

    decoder = null;
  }

  private File findNextFile() {
    logger.debug("Attempting to find a WAL file to read");

    File file = null;

    String[] walFiles = directory.list(new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".wal");
      }
    });

    if (walFiles != null && walFiles.length > 0) {
      List<String> walFilesList = Arrays.asList(walFiles);

      /*
       * Sort the WAL files by timestamp and select the first in an attempt to
       * preserve ordering.
       */
      Collections.sort(walFilesList, new Comparator<String>() {

        @Override
        public int compare(String o1, String o2) {
          Long o1L = Long.parseLong(o1.substring(0, o1.indexOf(".wal")));
          Long o2L = Long.parseLong(o2.substring(0, o2.indexOf(".wal")));

          return o1L.compareTo(o2L);
        }

      });

      logger.debug("Sorted list of WAL files:{}", walFilesList);

      if (index.getReadFile() != null) {
        String readFileName = new File(index.getReadFile()).getName();
        long currentFileTs = Long.parseLong(readFileName.substring(0,
            readFileName.indexOf(".wal")));

        logger.debug("Current WAL file timestamp:{}", currentFileTs);

        for (String walFile : walFilesList) {
          long ts = Long
              .parseLong(walFile.substring(0, walFile.indexOf(".wal")));

          if (ts > currentFileTs) {
            file = new File(walFile);
            logger.debug("WAL file ts:{} > currentFileTs:{} - selected!", ts,
                currentFileTs);
            break;
          }
        }

      } else {
        file = new File(directory, walFilesList.get(0));
      }
    }

    logger.debug("Selected write ahead log {} for reading", file);

    return file;
  }

  @Override
  public WALEntry next() {
    WALEntry entry = null;

    if (ensureWALFile()) {
      while (entry == null) {
        try {
          if (dataAvailable()) {
            entry = new AvroWALEntryAdapter(reader.read(null, decoder));
            currentPosition = inputChannel.position();

            if (logger.isDebugEnabled()) {
              logger.debug("Read entry:{} markPosition:{} currentPosition:{}",
                  new Object[] { entry, index.getReadPosition(),
                      currentPosition });
            }
          }
        } catch (EOFException e) {
          /*
           * Hitting EOF means we finished the WAL. There may be another WAL
           * waiting for us (i.e. the writer is generating new data) so we
           * attempt to locate a new file and open it. If we find a new file, we
           * retry the read, otherwise, we break out (i.e. no new data).
           */
          closeWALFile();

          if (!ensureWALFile()) {
            break;
          }
        } catch (IOException e) {
          throw new WALException("Failed to read WAL entry - file:"
              + currentFile + " position:" + currentPosition, e);
        }
      }
    }

    return entry;
  }

  private boolean dataAvailable() {
    synchronized (index) {
      if (!currentFile.getPath().equals(index.getWriteFile())
          || currentPosition < index.getWritePosition()) {
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public void mark() {
    logger.debug("Marking currentFile:{} currentPosition:{}", currentFile,
        currentPosition);

    index.updateReadIndex(currentFile.getPath(), currentPosition);
  }

  @Override
  public void reset() {
    logger.debug("Rewinding last successful read position");

    /*
     * It's possible we need to reset to a different file. Figure out if this is
     * a simple pointer move or a file close / open op.
     */
    if (currentFile.getPath().equals(index.getReadFile())) {
      currentPosition = index.getReadPosition();
    } else {
      openWALFile(new File(index.getReadFile()), index.getReadPosition());
    }
  }

  public long getCurrentPosition() {
    return currentPosition;
  }

  public long getMarkPosition() {
    return index.getReadPosition();
  }

  public WALIndex getIndex() {
    return index;
  }

  public void setIndex(WALIndex index) {
    this.index = index;
  }

  public File getDirectory() {
    return directory;
  }

  public void setDirectory(File directory) {
    this.directory = directory;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(getClass()).add("currentFile", currentFile)
        .add("currentPosition", currentPosition).add("directory", directory)
        .add("index", index).toString();
  }

}
