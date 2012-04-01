/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.hdfs;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.FlumeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class HDFSEventSink extends AbstractSink implements Configurable {
  private static final Logger LOG = LoggerFactory
      .getLogger(HDFSEventSink.class);

  static final long defaultRollInterval = 30;
  static final long defaultRollSize = 1024;
  static final long defaultRollCount = 10;
  static final String defaultFileName = "FlumeData";
  static final String defaultBucketFormat = "%yyyy-%mm-%dd/%HH";
  static final long defaultBatchSize = 1;
  static final long defaultTxnEventMax = 100;
  static final String defaultFileType = HDFSWriterFactory.SequenceFileType;
  static final int defaultMaxOpenFiles = 5000;
  /**
   * Default length of time we wait for an append
   * before closing the file and moving on.
   */
  static final long defaultAppendTimeout = 1000;
  /**
   * Default length of time we for a non-append
   * before closing the file and moving on. This
   * includes open/close/flush.
   */
  static final long defaultCallTimeout = 5000;
  /**
   * Default number of threads available for tasks
   * such as append/open/close/flush with hdfs.
   * These tasks are done in a separate thread in
   * the case that they take too long. In which
   * case we create a new file and move on.
   */
  static final int defaultThreadPoolSize = 10;

  private long rollInterval;
  private long rollSize;
  private long rollCount;
  private long txnEventMax;
  private long batchSize;
  private int threadsPoolSize;
  private CompressionCodec codeC;
  private CompressionType compType;
  private String fileType;
  private String path;
  private int maxOpenFiles;
  private String writeFormat;
  private HDFSWriterFactory myWriterFactory;
  private ExecutorService executor;
  private long appendTimeout;
  private String kerbConfPrincipal;
  private String kerbKeytabFile;
  private long callTimeout;

  /*
   * Extended Java LinkedHashMap for open file handle LRU queue We want to clear
   * the oldest file handle if there are too many open ones
   */
  private class WriterLinkedHashMap extends LinkedHashMap<String, BucketWriter> {
    private static final long serialVersionUID = 1L;

    @Override
    protected boolean removeEldestEntry(Entry<String, BucketWriter> eldest) {
      /*
       * FIXME: We probably shouldn't shared state this way. Make this class
       * private static and explicitly expose maxOpenFiles.
       */
      if (super.size() > maxOpenFiles) {
        // If we have more that max open files, then close the last one and
        // return true
        try {
          eldest.getValue().close();
        } catch (IOException eI) {
          LOG.warn(eldest.getKey().toString(), eI);
        }
        return true;
      } else {
        return false;
      }
    }
  }

  final WriterLinkedHashMap sfWriters = new WriterLinkedHashMap();

  // Used to short-circuit around doing regex matches when we know there are
  // no templates to be replaced.
  // private boolean shouldSub = false;

  public HDFSEventSink() {
    myWriterFactory = new HDFSWriterFactory();
  }

  public HDFSEventSink(HDFSWriterFactory newWriterFactory) {
    myWriterFactory = newWriterFactory;
  }

    // read configuration and setup thresholds
  @Override
  public void configure(Context context) {
    String dirpath = Preconditions.checkNotNull(
        context.getString("hdfs.path"), "hdfs.path is required");
    String fileName = context.getString("hdfs.filePrefix", defaultFileName);
    // FIXME: Not transportable.
    this.path = new String(dirpath + "/" + fileName);
    rollInterval = context.getLong("hdfs.rollInterval", defaultRollInterval);
    rollSize = context.getLong("hdfs.rollSize", defaultRollSize);
    rollCount = context.getLong("hdfs.rollCount", defaultRollCount);
    batchSize = context.getLong("hdfs.batchSize", defaultBatchSize);
    txnEventMax = context.getLong("hdfs.txnEventMax", defaultTxnEventMax);
    String codecName = context.getString("hdfs.codeC");
    fileType = context.getString("hdfs.fileType", defaultFileType);
    maxOpenFiles = context.getInteger("hdfs.maxOpenFiles", defaultMaxOpenFiles);
    writeFormat = context.getString("hdfs.writeFormat");
    appendTimeout = context.getLong("hdfs.appendTimeout", defaultAppendTimeout);
    callTimeout = context.getLong("hdfs.callTimeout", defaultCallTimeout);
    threadsPoolSize = context.getInteger("hdfs.threadsPoolSize", defaultThreadPoolSize);
    kerbConfPrincipal = context.getString("hdfs.kerberosPrincipal", "");
    kerbKeytabFile = context.getString("hdfs.kerberosKeytab", "");

    Preconditions.checkArgument(batchSize > 0,
        "batchSize must be greater than 0");
    Preconditions.checkArgument(txnEventMax > 0,
        "txnEventMax must be greater than 0");
    if (codecName == null) {
      codeC = null;
      compType = CompressionType.NONE;
    } else {
      codeC = getCodec(codecName);
      // TODO : set proper compression type
      compType = CompressionType.BLOCK;
    }

    if (writeFormat == null) {
      // Default write formatter is chosen by requested file type
      if(fileType.equalsIgnoreCase(HDFSWriterFactory.DataStreamType)
         || fileType.equalsIgnoreCase(HDFSWriterFactory.CompStreamType)) {
        // Output is written into text files, by default separate events by \n
        this.writeFormat = HDFSFormatterFactory.hdfsTextFormat;
      } else {
        // Output is written into binary files, so use binary writable format
        this.writeFormat = HDFSFormatterFactory.hdfsWritableFormat;
      }
    }

    boolean useSec = isHadoopSecurityEnabled();
    LOG.info("Hadoop Security enabled: " + useSec);

    if (useSec) {
      if (tryKerberosLogin()) {
        LOG.info("Successfully logged in via Kerberos.");
      } else {
        LOG.warn("Failed to log in via Kerberos.");
      }
    }
  }

  private static boolean codecMatches(Class<? extends CompressionCodec> cls,
      String codecName) {
    String simpleName = cls.getSimpleName();
    if (cls.getName().equals(codecName)
        || simpleName.equalsIgnoreCase(codecName)) {
      return true;
    }
    if (simpleName.endsWith("Codec")) {
      String prefix = simpleName.substring(0,
          simpleName.length() - "Codec".length());
      if (prefix.equalsIgnoreCase(codecName)) {
        return true;
      }
    }
    return false;
  }

  private static CompressionCodec getCodec(String codecName) {
    Configuration conf = new Configuration();
    List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory
        .getCodecClasses(conf);
    // Wish we could base this on DefaultCodec but appears not all codec's
    // extend DefaultCodec(Lzo)
    CompressionCodec codec = null;
    ArrayList<String> codecStrs = new ArrayList<String>();
    codecStrs.add("None");
    for (Class<? extends CompressionCodec> cls : codecs) {
      codecStrs.add(cls.getSimpleName());
      if (codecMatches(cls, codecName)) {
        try {
          codec = cls.newInstance();
        } catch (InstantiationException e) {
          LOG.error("Unable to instantiate " + cls + " class");
        } catch (IllegalAccessException e) {
          LOG.error("Unable to access " + cls + " class");
        }
      }
    }

    if (codec == null) {
      if (!codecName.equalsIgnoreCase("None")) {
        throw new IllegalArgumentException("Unsupported compression codec "
            + codecName + ".  Please choose from: " + codecStrs);
      }
    } else if (codec instanceof org.apache.hadoop.conf.Configurable) {
      // Must check instanceof codec as BZip2Codec doesn't inherit Configurable
      // Must set the configuration for Configurable objects that may or do use
      // native libs
      ((org.apache.hadoop.conf.Configurable) codec).setConf(conf);
    }
    return codec;
  }

  /**
   * Execute the callable on a separate thread and wait for the completion
   * for the specified amount of time in milliseconds. In case of timeout
   * or any other error, log error and return null.
   */
  private static <T> T callWithTimeoutLogError(final ExecutorService executor, long timeout, String name, final Callable<T> callable) {
    try {
      return callWithTimeout(executor, timeout, callable);
    } catch (Exception e) {
      LOG.error("Error calling " + callable, e);
      if(e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    }
    return null;
  }

  /**
   * Execute the callable on a separate thread and wait for the completion
   * for the specified amount of time in milliseconds. In case of timeout
   * cancel the callable and throw an IOException
   */
  private static <T> T callWithTimeout(final ExecutorService executor, long timeout, final Callable<T> callable)
      throws IOException, InterruptedException {
    Future<T> future = executor.submit(callable);
    try {
      if (timeout > 0) {
        return future.get(timeout, TimeUnit.MILLISECONDS);
      } else {
        return future.get();
      }
    } catch (TimeoutException eT) {
      future.cancel(true);
      throw new IOException("Callable timed out", eT);
    } catch (ExecutionException e1) {
      Throwable cause = e1.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof Error) {
        throw (Error)cause;
      } else {
        throw new RuntimeException(e1);
      }
    } catch (CancellationException ce) {
      throw new InterruptedException(
          "Blocked callable interrupted by rotation event");
    } catch (InterruptedException ex) {
      LOG.warn("Unexpected Exception " + ex.getMessage(), ex);
      throw (InterruptedException) ex;
    }
  }
  /**
   * Pull events out of channel and send it to HDFS - take at the most
   * txnEventMax, that's the maximum #events to hold in channel for a given
   * transaction - find the corresponding bucket for the event, ensure the file
   * is open - extract the pay-load and append to HDFS file
   */
  @Override
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    String realPath = null;
    List<BucketWriter> writers = Lists.newArrayList();
    try {
      transaction.begin();
      Event event = null;
      for (int txnEventCount = 0; txnEventCount < txnEventMax; txnEventCount++) {
        realPath = null;
        event = channel.take();
        if (event == null) {
          break;
        }

        // reconstruct the path name by substituting place holders
        realPath = BucketPath.escapeString(path, event.getHeaders());
        BucketWriter bucketWriter = sfWriters.get(realPath);

        // we haven't seen this file yet, so open it and cache the handle
        if (bucketWriter == null) {
          final HDFSWriter writer = myWriterFactory.getWriter(fileType);
          final FlumeFormatter formatter = HDFSFormatterFactory
              .getFormatter(writeFormat);
          bucketWriter = new BucketWriter(rollInterval, rollSize, rollCount, batchSize);
          final BucketWriter callableWriter = bucketWriter;
          final String callablePath = realPath;
          final CompressionCodec callableCodec = codeC;
          final CompressionType callableCompType = compType;
          callWithTimeout(executor, callTimeout, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              synchronized(callableWriter) {
                callableWriter.open(callablePath, callableCodec, callableCompType, writer, formatter);
              }
              return null;
            }
          });
          sfWriters.put(realPath, bucketWriter);
          writers.add(bucketWriter);
        }

        // Write the data to HDFS
        final BucketWriter callableWriter = bucketWriter;
        final Event callableEvent = event;
        callWithTimeout(executor, appendTimeout, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            synchronized(callableWriter) {
              try {
                callableWriter.append(callableEvent);
              } catch(IOException ex) {
                callableWriter.abort(); // close/open
                callableWriter.append(callableEvent); // retry
              }
              return null;
            }
          }
        });
      }

      transaction.commit();
      if(event == null) {
        return Status.BACKOFF;
      }
      return Status.READY;
    } catch (IOException eIO) {
      transaction.rollback();
      LOG.warn("HDFS IO error", eIO);
      return Status.BACKOFF;
    } catch (Throwable th) {
      transaction.rollback();
      LOG.error("process failed", th);
      if (th instanceof Error) {
        throw (Error) th;
      } else {
        throw new EventDeliveryException(th);
      }
    } finally {
      for (BucketWriter writer : writers) {
        final BucketWriter callableWriter = writer;
        LOG.info("Calling abort on " + callableWriter);
        callWithTimeoutLogError(executor, callTimeout, "Calling abort on "
        + callableWriter, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            synchronized(callableWriter) {
              callableWriter.abort();
            }
            return null;
          }
        });
      }
      transaction.close();
    }
  }

  @Override
  public void stop() {
    for (Entry<String, BucketWriter> e : sfWriters.entrySet()) {
      LOG.info("Closing " + e.getKey());
      final BucketWriter callableWriter = e.getValue();
      callWithTimeoutLogError(executor, callTimeout, "Closing " + e.getKey(), new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          synchronized(callableWriter) {
            callableWriter.close();
          }
          return null;
        }
      });
    }
    executor.shutdown();
    try {
      while (executor.isTerminated() == false) {
        executor.awaitTermination(Math.max(defaultCallTimeout, callTimeout),
            TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException ex) {
      LOG.warn("shutdown interrupted", ex);
    }
    executor = null;
    super.stop();
  }

  @Override
  public void start() {
    executor = Executors.newFixedThreadPool(threadsPoolSize);
    // XXX if restarted, the code below reopens the writers from the
    // previous "process" executions. Is this what we want? Why
    // not clear this list during close since the appropriate
    // writers will be opened in "process"?
    for (Entry<String, BucketWriter> e : sfWriters.entrySet()) {
      final BucketWriter callableWriter = e.getValue();
      callWithTimeoutLogError(executor, callTimeout, "Calling open on " +
      callableWriter, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          synchronized(callableWriter) {
            callableWriter.open();
          }
          return null;
        }
      });
    }
    super.start();
  }

  /**
   * Wraps call to Hadoop UserGroupInformation.isSecurityEnabled()
   * @return {@code true} if enabled, {@code false} if disabled or unavailable
   */
  private boolean isHadoopSecurityEnabled() {
    /*
     * UserGroupInformation is in hadoop 0.18
     * UserGroupInformation.isSecurityEnabled() not in pre security API.
     *
     * boolean enabled = UserGroupInformation.isSecurityEnabled();
     */
    boolean enabled;
    try {
      Class<UserGroupInformation> ugiC = UserGroupInformation.class;
      // static call, null this obj
      enabled = (Boolean) ugiC.getMethod("isSecurityEnabled").invoke(null);
    } catch (NoSuchMethodException e) {
      LOG.warn("Flume is using Hadoop core "
          + org.apache.hadoop.util.VersionInfo.getVersion()
          + " which does not support Security / Authentication: "
          + e.getMessage());
      return false;
    } catch (Exception e) {
      LOG.error("Unexpected error checking for Hadoop security support. " +
          "Exception follows.", e);
      return false;
    }

    return enabled;
  }

  /**
   * Attempt Kerberos login via a keytab if security is enabled in Hadoop.
   *
   * This method should only be called if {@link #isHadoopSecurityEnabled()}
   * returns true. This function assumes that is the case.
   *
   * This should be able to support multiple Hadoop clusters as long as the
   * particular principal is allowed on multiple clusters.
   *
   * To preserve compatibility with non-security enhanced HDFS, we use
   * reflection on various UserGroupInformation and SecurityUtil related method
   * calls.
   *
   * @return {@code true} if and only if login was successful.
   */
  private boolean tryKerberosLogin() {

    // attempt to load kerberos information for authenticated hdfs comms.
    LOG.info("Kerberos login as principal (" + kerbConfPrincipal + ") from " +
        "keytab file (" + kerbKeytabFile + ")");

    if (kerbConfPrincipal.isEmpty()) {
      LOG.error("Hadoop running in secure mode, but Flume config doesn't " +
          "specify a principal to use for Kerberos auth.");
      return false;
    }

    if (kerbKeytabFile.isEmpty()) {
      LOG.error("Hadoop running in secure mode, but Flume config doesn't " +
          "specify a keytab to use for Kerberos auth.");
      return false;
    }

    String principal;
    try {
      /*
       * SecurityUtil not present pre hadoop 20.2
       *
       * SecurityUtil.getServerPrincipal not in pre-security Hadoop API
       *
       * // resolves _HOST pattern using standard Hadoop search/replace
       * // via DNS lookup when 2nd argument is empty
       *
       * String principal = SecurityUtil.getServerPrincipal(confPrincipal, "");
       */
      @SuppressWarnings("rawtypes")
      Class suC = Class.forName("org.apache.hadoop.security.SecurityUtil");
      @SuppressWarnings("unchecked")
      Method m = suC.getMethod("getServerPrincipal", String.class, String.class);
      principal = (String) m.invoke(null, kerbConfPrincipal, "");
    } catch (Exception e) {
      LOG.error("Host lookup error resolving kerberos principal (" +
          kerbConfPrincipal + "). Exception follows.", e);
      return false;
    }

    try {
      /*
       * UserGroupInformation.loginUserFromKeytab not in pre-security Hadoop API
       *
       * // attempts to log user in using resolved principal
       *
       * UserGroupInformation.loginUserFromKeytab(principal, kerbKeytabFile);
       */
      Class<UserGroupInformation> ugi = UserGroupInformation.class;
      ugi.getMethod("loginUserFromKeytab", String.class, String.class)
          .invoke(null, principal, kerbKeytabFile);
    } catch (Exception e) {
      LOG.error("Authentication or file read error while attempting to " +
          "login as kerberos principal (" + principal + ") using keytab (" +
          kerbKeytabFile + "). Exception follows.", e);
      return false;
    }

    try {
      /*
       * getLoginUser, getAuthenticationMethod, and isLoginKeytabBased are not
       * in Hadoop 20.2, only kerberized enhanced version.
       *
       * getUserName is in all 0.18.3+
       *
       * UserGroupInformation ugi = UserGroupInformation.getLoginUser();
       * LOG.info("Auth method: " + ugi.getAuthenticationMethod());
       * LOG.info(" User name: " + ugi.getUserName());
       * LOG.info(" Using keytab: " +
       * UserGroupInformation.isLoginKeytabBased());
       */

      Class<UserGroupInformation> ugiC = UserGroupInformation.class;
      // static call, null this obj
      UserGroupInformation ugi = (UserGroupInformation) ugiC.getMethod(
          "getLoginUser").invoke(null);
      String authMethod = ugiC.getMethod("getAuthenticationMethod").invoke(ugi)
          .toString();
      boolean keytabBased = (Boolean) ugiC.getMethod("isLoginKeytabBased")
          .invoke(ugi);

      LOG.info("Auth method: " + authMethod);
      LOG.info(" User name: " + ugi.getUserName());
      LOG.info(" Using keytab: " + keytabBased);
    } catch (Exception e) {
      LOG.error("Flume was unable to dump kerberos login user"
          + " and authentication method", e);
    }

    return true;
  }
}