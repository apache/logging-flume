/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel.file;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.file.encryption.CipherProvider;
import org.apache.flume.channel.file.encryption.CipherProviderFactory;
import org.apache.flume.channel.file.encryption.DecryptionFailureException;
import org.apache.flume.channel.file.encryption.KeyProvider;
import org.apache.flume.channel.file.proto.ProtosFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.Key;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Represents a single data file on disk. Has methods to write,
 * read sequentially (replay), and read randomly (channel takes).
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LogFileV3 extends LogFile {
  protected static final Logger LOGGER =
      LoggerFactory.getLogger(LogFileV3.class);

  private LogFileV3() {}

  static class MetaDataWriter extends LogFile.MetaDataWriter {
    private ProtosFactory.LogFileMetaData logFileMetaData;
    private final File metaDataFile;
    protected MetaDataWriter(File logFile, int logFileID) throws IOException {
      super(logFile, logFileID);
      metaDataFile = Serialization.getMetaDataFile(logFile);
      MetaDataReader metaDataReader = new MetaDataReader(logFile, logFileID);
      logFileMetaData = metaDataReader.read();
      int version = logFileMetaData.getVersion();
      if(version != getVersion()) {
        throw new IOException("Version is " + Integer.toHexString(version) +
            " expected " + Integer.toHexString(getVersion())
            + " file: " + logFile);
      }
      setLastCheckpointOffset(logFileMetaData.getCheckpointPosition());
      setLastCheckpointWriteOrderID(logFileMetaData.getCheckpointWriteOrderID());
    }

    @Override
    int getVersion() {
      return Serialization.VERSION_3;
    }

    @Override
    void markCheckpoint(long currentPosition, long logWriteOrderID)
        throws IOException {
      ProtosFactory.LogFileMetaData.Builder metaDataBuilder =
          ProtosFactory.LogFileMetaData.newBuilder(logFileMetaData);
      metaDataBuilder.setCheckpointPosition(currentPosition);
      metaDataBuilder.setCheckpointWriteOrderID(logWriteOrderID);
      /*
       * Set the previous checkpoint position and write order id so that it
       * would be possible to recover from a backup.
       */
      metaDataBuilder.setBackupCheckpointPosition(logFileMetaData
        .getCheckpointPosition());
      metaDataBuilder.setBackupCheckpointWriteOrderID(logFileMetaData
        .getCheckpointWriteOrderID());
      logFileMetaData = metaDataBuilder.build();
      writeDelimitedTo(logFileMetaData, metaDataFile);
    }
  }

  static class MetaDataReader {
    private final File logFile;
    private final File metaDataFile;
    private final int logFileID;
    protected MetaDataReader(File logFile, int logFileID) throws IOException {
      this.logFile = logFile;
      metaDataFile = Serialization.getMetaDataFile(logFile);
      this.logFileID = logFileID;
    }
    ProtosFactory.LogFileMetaData read() throws IOException {
      FileInputStream inputStream = new FileInputStream(metaDataFile);
      try {
        ProtosFactory.LogFileMetaData metaData = Preconditions.checkNotNull(
          ProtosFactory.LogFileMetaData.
            parseDelimitedFrom(inputStream), "Metadata cannot be null");
        if (metaData.getLogFileID() != logFileID) {
          throw new IOException("The file id of log file: "
              + logFile + " is different from expected "
              + " id: expected = " + logFileID + ", found = "
              + metaData.getLogFileID());
        }
        return metaData;
      } finally {
        try {
          inputStream.close();
        } catch(IOException e) {
          LOGGER.warn("Unable to close " + metaDataFile, e);
        }
      }
    }
  }

  /**
   * Writes a GeneratedMessage to a temp file, synchronizes it to disk
   * and then renames the file over file.
   * @param msg GeneratedMessage to write to the file
   * @param file destination file
   * @throws IOException if a write error occurs or the File.renameTo
   * method returns false meaning the file could not be overwritten.
   */
  public static void writeDelimitedTo(GeneratedMessage msg, File file)
  throws IOException {
    File tmp = Serialization.getMetaDataTempFile(file);
    FileOutputStream outputStream = new FileOutputStream(tmp);
    boolean closed = false;
    try {
      msg.writeDelimitedTo(outputStream);
      outputStream.getChannel().force(true);
      outputStream.close();
      closed = true;
      if(!tmp.renameTo(file)) {
        //Some platforms don't support moving over an existing file.
        //So:
        //log.meta -> log.meta.old
        //log.meta.tmp -> log.meta
        //delete log.meta.old
        File oldFile = Serialization.getOldMetaDataFile(file);
        if(!file.renameTo(oldFile)){
          throw new IOException("Unable to rename " + file + " to " + oldFile);
        }
        if(!tmp.renameTo(file)) {
          throw new IOException("Unable to rename " + tmp + " over " + file);
        }
        oldFile.delete();
      }
    } finally {
      if(!closed) {
        try {
          outputStream.close();
        } catch(IOException e) {
          LOGGER.warn("Unable to close " + tmp, e);
        }
      }
    }

  }

  static class Writer extends LogFile.Writer {
    Writer(File file, int logFileID, long maxFileSize,
        @Nullable Key encryptionKey,
        @Nullable String encryptionKeyAlias,
        @Nullable String encryptionCipherProvider,
        long usableSpaceRefreshInterval, boolean fsyncPerTransaction,
        int fsyncInterval) throws IOException {
      super(file, logFileID, maxFileSize, CipherProviderFactory.
          getEncrypter(encryptionCipherProvider, encryptionKey),
          usableSpaceRefreshInterval, fsyncPerTransaction, fsyncInterval);
      ProtosFactory.LogFileMetaData.Builder metaDataBuilder =
          ProtosFactory.LogFileMetaData.newBuilder();
      if(encryptionKey != null) {
        Preconditions.checkNotNull(encryptionKeyAlias, "encryptionKeyAlias");
        Preconditions.checkNotNull(encryptionCipherProvider,
            "encryptionCipherProvider");
        ProtosFactory.LogFileEncryption.Builder logFileEncryptionBuilder =
            ProtosFactory.LogFileEncryption.newBuilder();
        logFileEncryptionBuilder.setCipherProvider(encryptionCipherProvider);
        logFileEncryptionBuilder.setKeyAlias(encryptionKeyAlias);
        logFileEncryptionBuilder.setParameters(
            ByteString.copyFrom(getEncryptor().getParameters()));
        metaDataBuilder.setEncryption(logFileEncryptionBuilder);
      }
      metaDataBuilder.setVersion(getVersion());
      metaDataBuilder.setLogFileID(logFileID);
      metaDataBuilder.setCheckpointPosition(0L);
      metaDataBuilder.setCheckpointWriteOrderID(0L);
      metaDataBuilder.setBackupCheckpointPosition(0L);
      metaDataBuilder.setBackupCheckpointWriteOrderID(0L);
      File metaDataFile = Serialization.getMetaDataFile(file);
      writeDelimitedTo(metaDataBuilder.build(), metaDataFile);
    }
    @Override
    int getVersion() {
      return Serialization.VERSION_3;
    }
  }

  static class RandomReader extends LogFile.RandomReader {
    private volatile boolean initialized;
    private volatile boolean encryptionEnabled;
    private volatile Key key;
    private volatile String cipherProvider;
    private volatile byte[] parameters;
    private BlockingQueue<CipherProvider.Decryptor> decryptors =
      new LinkedBlockingDeque<CipherProvider.Decryptor>();

    RandomReader(File file, @Nullable KeyProvider encryptionKeyProvider,
      boolean fsyncPerTransaction) throws IOException {
      super(file, encryptionKeyProvider, fsyncPerTransaction);
    }
    private void initialize() throws IOException {
      File metaDataFile = Serialization.getMetaDataFile(getFile());
      FileInputStream inputStream = new FileInputStream(metaDataFile);
      try {
        ProtosFactory.LogFileMetaData metaData =
            Preconditions.checkNotNull(ProtosFactory.LogFileMetaData.
                parseDelimitedFrom(inputStream), "MetaData cannot be null");
        int version = metaData.getVersion();
        if(version != getVersion()) {
          throw new IOException("Version is " + Integer.toHexString(version) +
              " expected " + Integer.toHexString(getVersion())
              + " file: " + getFile().getCanonicalPath());
        }
        encryptionEnabled = false;
        if(metaData.hasEncryption()) {
          if(getKeyProvider() == null) {
            throw new IllegalStateException("Data file is encrypted but no " +
                " provider was specified");
          }
          ProtosFactory.LogFileEncryption encryption = metaData.getEncryption();
          key = getKeyProvider().getKey(encryption.getKeyAlias());
          cipherProvider = encryption.getCipherProvider();
          parameters = encryption.getParameters().toByteArray();
          encryptionEnabled = true;
        }
      } finally {
        try {
          inputStream.close();
        } catch(IOException e) {
          LOGGER.warn("Unable to close " + metaDataFile, e);
        }
      }
    }
    private CipherProvider.Decryptor getDecryptor() {
      CipherProvider.Decryptor decryptor = decryptors.poll();
      if(decryptor == null) {
        decryptor = CipherProviderFactory.getDecrypter(cipherProvider, key,
            parameters);
      }
      return decryptor;
    }
    @Override
    int getVersion() {
      return Serialization.VERSION_3;
    }
    @Override
    protected TransactionEventRecord doGet(RandomAccessFile fileHandle)
        throws IOException, CorruptEventException {
      // readers are opened right when the file is created and thus
      // empty. As such we wait to initialize until there is some
      // data before we we initialize
      synchronized (this) {
        if(!initialized) {
          initialized = true;
          initialize();
        }
      }
      boolean success = false;
      CipherProvider.Decryptor decryptor = null;
      try {
        byte[] buffer = readDelimitedBuffer(fileHandle);
        if(encryptionEnabled) {
          decryptor = getDecryptor();
          buffer = decryptor.decrypt(buffer);
        }
        TransactionEventRecord event = TransactionEventRecord.
            fromByteArray(buffer);
        success = true;
        return event;
      } catch(DecryptionFailureException ex) {
        throw new CorruptEventException("Error decrypting event", ex);
      } finally {
        if(success && encryptionEnabled && decryptor != null) {
          decryptors.offer(decryptor);
        }
      }
    }
  }

  public static class SequentialReader extends LogFile.SequentialReader {
    private CipherProvider.Decryptor decryptor;
    private final boolean fsyncPerTransaction;
    public SequentialReader(File file, @Nullable KeyProvider
      encryptionKeyProvider, boolean fsyncPerTransaction) throws EOFException,
      IOException {
      super(file, encryptionKeyProvider);
      this.fsyncPerTransaction = fsyncPerTransaction;
      File metaDataFile = Serialization.getMetaDataFile(file);
      FileInputStream inputStream = new FileInputStream(metaDataFile);
      try {
        ProtosFactory.LogFileMetaData metaData = Preconditions.checkNotNull(
            ProtosFactory.LogFileMetaData.parseDelimitedFrom(inputStream),
            "MetaData cannot be null");
        int version = metaData.getVersion();
        if(version != getVersion()) {
          throw new IOException("Version is " + Integer.toHexString(version) +
              " expected " + Integer.toHexString(getVersion())
              + " file: " + file.getCanonicalPath());
        }
        if(metaData.hasEncryption()) {
          if(getKeyProvider() == null) {
            throw new IllegalStateException("Data file is encrypted but no " +
                " provider was specified");
          }
          ProtosFactory.LogFileEncryption encryption = metaData.getEncryption();
          Key key = getKeyProvider().getKey(encryption.getKeyAlias());
          decryptor = CipherProviderFactory.
              getDecrypter(encryption.getCipherProvider(), key,
                  encryption.getParameters().toByteArray());
        }
        setLogFileID(metaData.getLogFileID());
        setLastCheckpointPosition(metaData.getCheckpointPosition());
        setLastCheckpointWriteOrderID(metaData.getCheckpointWriteOrderID());
        setPreviousCheckpointPosition(metaData.getBackupCheckpointPosition());
        setPreviousCheckpointWriteOrderID(
          metaData.getBackupCheckpointWriteOrderID());
      } finally {
        try {
          inputStream.close();
        } catch(IOException e) {
          LOGGER.warn("Unable to close " + metaDataFile, e);
        }
      }
    }

    @Override
    public int getVersion() {
      return Serialization.VERSION_3;
    }

    @Override
    LogRecord doNext(int offset) throws IOException, CorruptEventException,
      DecryptionFailureException {
      byte[] buffer = null;
      TransactionEventRecord event = null;
      try {
        buffer = readDelimitedBuffer(getFileHandle());
        if (decryptor != null) {
          buffer = decryptor.decrypt(buffer);
        }
        event = TransactionEventRecord.fromByteArray(buffer);
      } catch (CorruptEventException ex) {
        LOGGER.warn("Corrupt file found. File id: log-" + this.getLogFileID(),
          ex);
        // Return null so that replay handler thinks all events in this file
        // have been taken.
        if (!fsyncPerTransaction) {
          return null;
        }
        throw ex;
      } catch (DecryptionFailureException ex) {
        if (!fsyncPerTransaction) {
          LOGGER.warn("Could not decrypt even read from channel. Skipping " +
            "event.", ex);
          return null;
        }
        throw ex;
      }
      return new LogRecord(getLogFileID(), offset, event);
    }
  }
}
