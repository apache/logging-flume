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
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Calendar;
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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.FlumeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class HDFSEventSink extends AbstractSink implements Configurable {
  private static final Logger LOG = LoggerFactory
      .getLogger(HDFSEventSink.class);

  private static final long defaultRollInterval = 30;
  private static final long defaultRollSize = 1024;
  private static final long defaultRollCount = 10;
  private static final String defaultFileName = "FlumeData";
  private static final long defaultBatchSize = 1;
  private static final long defaultTxnEventMax = 100;
  private static final String defaultFileType = HDFSWriterFactory.SequenceFileType;
  private static final int defaultMaxOpenFiles = 5000;
  /**
   * Default length of time we wait for an append
   * before closing the file and moving on.
   */
  private static final long defaultAppendTimeout = 1000;
  /**
   * Default length of time we for a non-append
   * before closing the file and moving on. This
   * includes open/close/flush.
   */
  private static final long defaultCallTimeout = 5000;
  /**
   * Default number of threads available for tasks
   * such as append/open/close/flush with hdfs.
   * These tasks are done in a separate thread in
   * the case that they take too long. In which
   * case we create a new file and move on.
   */
  private static final int defaultThreadPoolSize = 10;

  /**
   * Singleton credential manager that manages static credentials for the
   * entire JVM
   */
  private static final AtomicReference<KerberosUser> staticLogin
      = new AtomicReference<KerberosUser>();

  private final HDFSWriterFactory writerFactory;
  private final WriterLinkedHashMap sfWriters;

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
  private ExecutorService executor;
  private long appendTimeout;

  private String kerbConfPrincipal;
  private String kerbKeytab;
  private String proxyUserName;
  private UserGroupInformation proxyTicket;

  private boolean needRounding = false;
  private int roundUnit = Calendar.SECOND;
  private int roundValue = 1;

  private long callTimeout;
  private Context context;

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

  /**
   * Helper class to wrap authentication calls.
   * @param <T> generally should be {@link Void}
   */
  private static abstract class ProxyCallable<T> implements Callable {
    private UserGroupInformation proxyTicket;

    public ProxyCallable(UserGroupInformation proxyTicket) {
      this.proxyTicket = proxyTicket;
    }

    @Override
    public T call() throws Exception {
      if (proxyTicket == null) {
        return doCall();
      } else {
        return proxyTicket.doAs(new PrivilegedExceptionAction<T>() {

          @Override
          public T run() throws Exception {
            return doCall();
          }
        });
      }
    }

    abstract public T doCall() throws Exception;
  }


  public HDFSEventSink() {
    this(new HDFSWriterFactory());
  }

  public HDFSEventSink(HDFSWriterFactory writerFactory) {
    this.writerFactory = writerFactory;
    this.sfWriters = new WriterLinkedHashMap();
  }

    // read configuration and setup thresholds
  @Override
  public void configure(Context context) {
    this.context = context;

    String dirpath = Preconditions.checkNotNull(
        context.getString("hdfs.path"), "hdfs.path is required");
    String fileName = context.getString("hdfs.filePrefix", defaultFileName);
    // FIXME: Not portable to Windows
    this.path = dirpath + "/" + fileName;
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
    kerbKeytab = context.getString("hdfs.kerberosKeytab", "");
    proxyUserName = context.getString("hdfs.proxyUser", "");

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

    if (!authenticate(path)) {
      LOG.error("Failed to authenticate!");
    }
    needRounding = context.getBoolean("hdfs.round", false);

    if(needRounding) {
      String unit = context.getString("hdfs.roundUnit", "second");
      if (unit.equalsIgnoreCase("hour")) {
        this.roundUnit = Calendar.HOUR_OF_DAY;
      } else if (unit.equalsIgnoreCase("minute")) {
        this.roundUnit = Calendar.MINUTE;
      } else if (unit.equalsIgnoreCase("second")){
        this.roundUnit = Calendar.SECOND;
      } else {
        LOG.warn("Rounding unit is not valid, please set one of" +
            "minute, hour, or second. Rounding will be disabled");
        needRounding = false;
      }
      this.roundValue = context.getInteger("hdfs.roundValue", 1);
      if(roundUnit == Calendar.SECOND || roundUnit == Calendar.MINUTE){
        Preconditions.checkArgument(roundValue > 0 && roundValue <= 60,
            "Round value" +
            "must be > 0 and <= 60");
      } else if (roundUnit == Calendar.HOUR_OF_DAY){
        Preconditions.checkArgument(roundValue > 0 && roundValue <= 24,
            "Round value" +
            "must be > 0 and <= 24");
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
  private static <T> T callWithTimeoutLogError(final ExecutorService executor,
      long timeout, String name, final Callable<T> callable) {
    try {
      return callWithTimeout(executor, timeout, callable);
    } catch (Exception e) {
      LOG.error(name + "; called " + callable, e);
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
  private static <T> T callWithTimeout(final ExecutorService executor,
      long timeout, final Callable<T> callable)
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
      throw ex;
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
    List<BucketWriter> writers = Lists.newArrayList();
    try {
      transaction.begin();
      Event event = null;
      for (int txnEventCount = 0; txnEventCount < txnEventMax; txnEventCount++) {
        event = channel.take();
        if (event == null) {
          break;
        }

        // reconstruct the path name by substituting place holders
        String realPath = BucketPath.escapeString(path, event.getHeaders(),
            needRounding, roundUnit, roundValue);
        BucketWriter bucketWriter = sfWriters.get(realPath);

        // we haven't seen this file yet, so open it and cache the handle
        if (bucketWriter == null) {
          final HDFSWriter writer = writerFactory.getWriter(fileType);
          final FlumeFormatter formatter = HDFSFormatterFactory
              .getFormatter(writeFormat);
          bucketWriter = new BucketWriter(rollInterval, rollSize, rollCount, batchSize, context);
          final BucketWriter callableWriter = bucketWriter;
          final String callablePath = realPath;
          final CompressionCodec callableCodec = codeC;
          final CompressionType callableCompType = compType;

          callWithTimeout(executor, callTimeout,
              new ProxyCallable<Void>(proxyTicket) {
            @Override
            public Void doCall() throws Exception {
              synchronized(callableWriter) {
                callableWriter.open(callablePath, callableCodec,
                    callableCompType, writer, formatter);
              }
              return null;
            }
          });
          sfWriters.put(realPath, bucketWriter);
        }

        // track the buckets getting written in this transaction
        if (!writers.contains(bucketWriter)) {
          writers.add(bucketWriter);
        }

        // Write the data to HDFS
        final BucketWriter callableWriter = bucketWriter;
        final Event callableEvent = event;
        callWithTimeout(executor, appendTimeout,
            new ProxyCallable<Void>(proxyTicket) {
          @Override
          public Void doCall() throws Exception {
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
      // flush the buckets that still has pending data
      // this ensures that the data removed from channel
      // by the current transaction is safely on disk
      for (BucketWriter writer : writers) {
        if (writer.isBatchComplete()) {
          continue;
        }
        final BucketWriter callableWriter = writer;
        callWithTimeoutLogError(executor, callTimeout, "flush on "
        + callableWriter, new ProxyCallable<Void>(proxyTicket) {
          @Override
          public Void doCall() throws Exception {
            synchronized(callableWriter) {
              callableWriter.flush();
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
      callWithTimeoutLogError(executor, callTimeout, "close on " + e.getKey(),
          new ProxyCallable<Void>(proxyTicket) {
        @Override
        public Void doCall() throws Exception {
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
    // FIXME: if restarted, the code below reopens the writers from the
    // previous "process" executions. Is this what we want? Why
    // not clear this list during close since the appropriate
    // writers will be opened in "process"?
    // TODO: check if this has anything to do with renaming .tmp files.
    // If not, remove this code.
    for (Entry<String, BucketWriter> entry : sfWriters.entrySet()) {
      final BucketWriter callableWriter = entry.getValue();
      try {
        callWithTimeout(executor, callTimeout,
            new ProxyCallable<Void>(proxyTicket) {
          @Override
          public Void doCall() throws Exception {
            synchronized(callableWriter) {
              callableWriter.open();
            }
            return null;
          }
        });
      } catch (IOException e) {
        throw new FlumeException("Exception opening HDFS file.", e);
      } catch (InterruptedException e) {
        throw new FlumeException("Interrupt while opening HDFS file.", e);
      }
    }
    super.start();
  }

  private boolean authenticate(String hdfsPath) {

    // logic for kerberos login
    boolean useSecurity = UserGroupInformation.isSecurityEnabled();

    LOG.info("Hadoop Security enabled: " + useSecurity);

    if (useSecurity) {

      // sanity checking
      if (kerbConfPrincipal.isEmpty()) {
        LOG.error("Hadoop running in secure mode, but Flume config doesn't "
            + "specify a principal to use for Kerberos auth.");
        return false;
      }
      if (kerbKeytab.isEmpty()) {
        LOG.error("Hadoop running in secure mode, but Flume config doesn't "
            + "specify a keytab to use for Kerberos auth.");
        return false;
      }

      String principal;
      try {
        // resolves _HOST pattern using standard Hadoop search/replace
        // via DNS lookup when 2nd argument is empty
        principal = SecurityUtil.getServerPrincipal(kerbConfPrincipal, "");
      } catch (IOException e) {
        LOG.error("Host lookup error resolving kerberos principal ("
            + kerbConfPrincipal + "). Exception follows.", e);
        return false;
      }

      Preconditions.checkNotNull(principal, "Principal must not be null");
      KerberosUser prevUser = staticLogin.get();
      KerberosUser newUser = new KerberosUser(principal, kerbKeytab);

      // be cruel and unusual when user tries to login as multiple principals
      // this isn't really valid with a reconfigure but this should be rare
      // enough to warrant a restart of the agent JVM
      // TODO: find a way to interrogate the entire current config state,
      // since we don't have to be unnecessarily protective if they switch all
      // HDFS sinks to use a different principal all at once.
      Preconditions.checkState(prevUser == null || prevUser.equals(newUser),
          "Cannot use multiple kerberos principals in the same agent. " +
          " Must restart agent to use new principal or keytab. " +
          "Previous = %s, New = %s", prevUser, newUser);

      // attempt to use cached credential if the user is the same
      // this is polite and should avoid flooding the KDC with auth requests
      UserGroupInformation curUser = null;
      if (prevUser != null && prevUser.equals(newUser)) {
        try {
          curUser = UserGroupInformation.getLoginUser();
        } catch (IOException e) {
          LOG.warn("User unexpectedly had no active login. Continuing with " +
              "authentication", e);
        }
      }

      if (curUser == null || !curUser.getUserName().equals(principal)) {
        try {
          // static login
          kerberosLogin(principal, kerbKeytab);
        } catch (IOException e) {
          LOG.error("Authentication or file read error while attempting to "
              + "login as kerberos principal (" + principal + ") using "
              + "keytab (" + kerbKeytab + "). Exception follows.", e);
        }
      }

      // we supposedly got through this unscathed... so store the static user
      staticLogin.set(newUser);
    }

    // hadoop impersonation works with or without kerberos security
    proxyTicket = null;
    if (!proxyUserName.isEmpty()) {
      try {
        proxyTicket = UserGroupInformation.createProxyUser(
            proxyUserName, UserGroupInformation.getLoginUser());
      } catch (IOException e) {
        LOG.error("Unable to login as proxy user. Exception follows.", e);
        return false;
      }
    }

    UserGroupInformation ugi = null;
    if (proxyTicket != null) {
      ugi = proxyTicket;
    } else if (useSecurity) {
      try {
        ugi = UserGroupInformation.getLoginUser();
      } catch (IOException e) {
        LOG.error("Unexpected error: Unable to get authenticated user after " +
            "apparent successful login! Exception follows.", e);
        return false;
      }
    }

    if (ugi != null) {
      // dump login information
      AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
      LOG.info("Auth method: {}", authMethod);
      LOG.info(" User name: {}", ugi.getUserName());
      LOG.info(" Using keytab: {}", ugi.isFromKeytab());
      if (authMethod == AuthenticationMethod.PROXY) {
        UserGroupInformation superUser;
        try {
          superUser = UserGroupInformation.getLoginUser();
          LOG.info(" Superuser auth: {}", superUser.getAuthenticationMethod());
          LOG.info(" Superuser name: {}", superUser.getUserName());
          LOG.info(" Superuser using keytab: {}", superUser.isFromKeytab());
        } catch (IOException e) {
          LOG.error("Unexpected error: unknown superuser impersonating proxy.",
              e);
          return false;
        }
      }

      LOG.info("Logged in as user {}", ugi.getUserName());

      return true;
    }

    return true;
  }

  /**
   * Static synchronized method for static Kerberos login. <br/>
   * Static synchronized due to a thundering herd problem when multiple Sinks
   * attempt to log in using the same principal at the same time with the
   * intention of impersonating different users (or even the same user).
   * If this is not controlled, MIT Kerberos v5 believes it is seeing a replay
   * attach and it returns:
   * <blockquote>Request is a replay (34) - PROCESS_TGS</blockquote>
   * In addition, since the underlying Hadoop APIs we are using for
   * impersonation are static, we define this method as static as well.
   *
   * @param principal Fully-qualified principal to use for authentication.
   * @param keytab Location of keytab file containing credentials for principal.
   * @return Logged-in user
   * @throws IOException if login fails.
   */
  private static synchronized UserGroupInformation kerberosLogin(
      String principal, String keytab) throws IOException {

    // if we are the 2nd user thru the lock, the login should already be
    // available statically if login was successful
    UserGroupInformation curUser = null;
    try {
      curUser = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      // not a big deal but this shouldn't typically happen because it will
      // generally fall back to the UNIX user
      LOG.debug("Unable to get login user before Kerberos auth attempt.", e);
    }

    // this means we really actually have not logged in successfully yet
    if (curUser == null || !curUser.getUserName().equals(principal)) {

      LOG.info("Attempting kerberos login as principal (" + principal + ") " +
          "from keytab file (" + keytab + ")");

      // attempt static kerberos login
      UserGroupInformation.loginUserFromKeytab(principal, keytab);
      curUser = UserGroupInformation.getLoginUser();
    }

    return curUser;
  }

}
