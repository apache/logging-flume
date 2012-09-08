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

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.Key;

import javax.annotation.Nullable;

import org.apache.flume.chanel.file.proto.ProtosFactory;
import org.apache.flume.channel.file.encryption.CipherProvider;
import org.apache.flume.channel.file.encryption.CipherProviderFactory;
import org.apache.flume.channel.file.encryption.KeyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

/**
 * Represents a single data file on disk. Has methods to write,
 * read sequentially (replay), and read randomly (channel takes).
 */
class LogFileV3 extends LogFile {
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
      logFileMetaData = metaDataBuilder.build();
      LOGGER.info("Updating " + metaDataFile.getName()  + " currentPosition = "
          + currentPosition + ", logWriteOrderID = " + logWriteOrderID);
      FileOutputStream outputStream = new FileOutputStream(metaDataFile);
      try {
        logFileMetaData.writeDelimitedTo(outputStream);
        outputStream.getChannel().force(true);
      } finally {
        try {
          outputStream.close();
        } catch(IOException e) {
          LOGGER.warn("Unable to close " + metaDataFile, e);
        }
      }
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
        ProtosFactory.LogFileMetaData metaData =
            ProtosFactory.LogFileMetaData.parseDelimitedFrom(inputStream);
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

  static class Writer extends LogFile.Writer {
    Writer(File file, int logFileID, long maxFileSize,
        @Nullable Key encryptionKey,
        @Nullable String encryptionKeyAlias,
        @Nullable String encryptionCipherProvider)
        throws IOException {
      super(file, logFileID, maxFileSize, CipherProviderFactory.
          getEncrypter(encryptionCipherProvider, encryptionKey));
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
      File metaDataFile = Serialization.getMetaDataFile(file);
      FileOutputStream outputStream = new FileOutputStream(metaDataFile);
      try {
        metaDataBuilder.build().writeDelimitedTo(outputStream);
        outputStream.getChannel().force(true);
      } finally {
        try {
          outputStream.close();
        } catch(IOException e) {
          LOGGER.warn("Unable to close " + metaDataFile, e);
        }
      }
    }
    @Override
    int getVersion() {
      return Serialization.VERSION_3;
    }
  }

  static class RandomReader extends LogFile.RandomReader {
    private volatile boolean initialized;
    private CipherProvider.Decryptor decryptor;
    RandomReader(File file, @Nullable KeyProvider encryptionKeyProvider)
        throws IOException {
      super(file, encryptionKeyProvider);
    }
    private void initialize() throws IOException {
      File metaDataFile = Serialization.getMetaDataFile(getFile());
      FileInputStream inputStream = new FileInputStream(metaDataFile);
      try {
        ProtosFactory.LogFileMetaData metaData =
            ProtosFactory.LogFileMetaData.parseDelimitedFrom(inputStream);
        int version = metaData.getVersion();
        if(version != getVersion()) {
          throw new IOException("Version is " + Integer.toHexString(version) +
              " expected " + Integer.toHexString(getVersion())
              + " file: " + getFile().getCanonicalPath());
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
      } finally {
        try {
          inputStream.close();
        } catch(IOException e) {
          LOGGER.warn("Unable to close " + metaDataFile, e);
        }
      }
    }
    @Override
    int getVersion() {
      return Serialization.VERSION_3;
    }
    @Override
    protected TransactionEventRecord doGet(RandomAccessFile fileHandle)
        throws IOException {
      // readers are opened right when the file is created and thus
      // empty. As such we wait to initialize until there is some
      // data before we we initialize
      if(!initialized) {
        initialized = true;
        initialize();
      }
      byte[] buffer = readDelimitedBuffer(fileHandle);
      if(decryptor != null) {
        buffer = decryptor.decrypt(buffer);
      }
      return TransactionEventRecord.fromByteArray(buffer);
    }
  }

  static class SequentialReader extends LogFile.SequentialReader {
    private CipherProvider.Decryptor decryptor;
    SequentialReader(File file, @Nullable KeyProvider encryptionKeyProvider)
        throws EOFException, IOException {
      super(file, encryptionKeyProvider);
      File metaDataFile = Serialization.getMetaDataFile(file);
      FileInputStream inputStream = new FileInputStream(metaDataFile);
      try {
        ProtosFactory.LogFileMetaData metaData =
            ProtosFactory.LogFileMetaData.parseDelimitedFrom(inputStream);
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
    LogRecord doNext(int offset) throws IOException {
      byte[] buffer = readDelimitedBuffer(getFileHandle());
      if(decryptor != null) {
        buffer = decryptor.decrypt(buffer);
      }
      TransactionEventRecord event =
          TransactionEventRecord.fromByteArray(buffer);
      return new LogRecord(getLogFileID(), offset, event);
    }
  }
}
