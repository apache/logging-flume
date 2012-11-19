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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
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
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class HDFSEventSink extends AbstractSink implements Configurable {
  public interface WriterCallback {
    public void run(String filePath);
  }

  private static final Logger LOG = LoggerFactory
      .getLogger(HDFSEventSink.class);

  private static final long defaultRollInterval = 30;
  private static final long defaultRollSize = 1024;
  private static final long defaultRollCount = 10;
  private static final String defaultFileName = "FlumeData";
  private static final String defaultSuffix = "";
  private static final long defaultBatchSize = 100;
  private static final long defaultTxnEventMax = 100;
  private static final String defaultFileType = HDFSWriterFactory.SequenceFileType;
  private static final int defaultMaxOpenFiles = 5000;
  /**
   * Default length of time we wait for blocking BucketWriter calls
   * before timing out the operation. Intended to prevent server hangs.
   */
  private static final long defaultCallTimeout = 10000;
  /**
   * Default number of threads available for tasks
   * such as append/open/close/flush with hdfs.
   * These tasks are done in a separate thread in
   * the case that they take too long. In which
   * case we create a new file and move on.
   */
  private static final int defaultThreadPoolSize = 10;
  private static final int defaultRollTimerPoolSize = 1;

  /**
   * Singleton credential manager that manages static credentials for the
   * entire JVM
   */
  private static final AtomicReference<KerberosUser> staticLogin
      = new AtomicReference<KerberosUser>();

  private final HDFSWriterFactory writerFactory;
  private WriterLinkedHashMap sfWriters;

  private long rollInterval;
  private long rollSize;
  private long rollCount;
  private long batchSize;
  private int threadsPoolSize;
  private int rollTimerPoolSize;
  private CompressionCodec codeC;
  private CompressionType compType;
  private String fileType;
  private String path;
  private String suffix;
  private TimeZone timeZone;
  private int maxOpenFiles;
  private String writeFormat;
  private ExecutorService callTimeoutPool;
  private ScheduledExecutorService timedRollerPool;

  private String kerbConfPrincipal;
  private String kerbKeytab;
  private String proxyUserName;
  private UserGroupInformation proxyTicket;

  private boolean needRounding = false;
  private int roundUnit = Calendar.SECOND;
  private int roundValue = 1;

  private long callTimeout;
  private Context context;
  private SinkCounter sinkCounter;

  private volatile int idleTimeout;

  /*
   * Extended Java LinkedHashMap for open file handle LRU queue.
   * We want to clear the oldest file handle if there are too many open ones.
   */
  private static class WriterLinkedHashMap
      extends LinkedHashMap<String, BucketWriter> {

    private final int maxOpenFiles;

    public WriterLinkedHashMap(int maxOpenFiles) {
      super(16, 0.75f, true); // stock initial capacity/load, access ordering
      this.maxOpenFiles = maxOpenFiles;
    }

    @Override
    protected boolean removeEldestEntry(Entry<String, BucketWriter> eldest) {
      if (size() > maxOpenFiles) {
        // If we have more that max open files, then close the last one and
        // return true
        try {
          eldest.getValue().close();
        } catch (IOException e) {
          LOG.warn(eldest.getKey().toString(), e);
        } catch (InterruptedException e) {
          LOG.warn(eldest.getKey().toString(), e);
          Thread.currentThread().interrupt();
        }
        return true;
      } else {
        return false;
      }
    }
  }

  public HDFSEventSink() {
    this(new HDFSWriterFactory());
  }

  public HDFSEventSink(HDFSWriterFactory writerFactory) {
    this.writerFactory = writerFactory;
  }

    // read configuration and setup thresholds
  public void configure(Context context) {
    this.context = context;

    String dirpath = Preconditions.checkNotNull(
        context.getString("hdfs.path"), "hdfs.path is required");
    String fileName = context.getString("hdfs.filePrefix", defaultFileName);
    // FLUME-1645: add suffix support
    this.suffix = context.getString("hdfs.fileSuffix", defaultSuffix);
    this.path = dirpath + System.getProperty("file.separator") + fileName;
    String tzName = context.getString("hdfs.timeZone");
    timeZone = tzName == null ? null : TimeZone.getTimeZone(tzName);
    rollInterval = context.getLong("hdfs.rollInterval", defaultRollInterval);
    rollSize = context.getLong("hdfs.rollSize", defaultRollSize);
    rollCount = context.getLong("hdfs.rollCount", defaultRollCount);
    batchSize = context.getLong("hdfs.batchSize", defaultBatchSize);
    idleTimeout = context.getInteger("hdfs.idleTimeout", 0);
    String codecName = context.getString("hdfs.codeC");
    fileType = context.getString("hdfs.fileType", defaultFileType);
    maxOpenFiles = context.getInteger("hdfs.maxOpenFiles", defaultMaxOpenFiles);
    writeFormat = context.getString("hdfs.writeFormat");
    callTimeout = context.getLong("hdfs.callTimeout", defaultCallTimeout);
    threadsPoolSize = context.getInteger("hdfs.threadsPoolSize",
        defaultThreadPoolSize);
    rollTimerPoolSize = context.getInteger("hdfs.rollTimerPoolSize",
        defaultRollTimerPoolSize);
    kerbConfPrincipal = context.getString("hdfs.kerberosPrincipal", "");
    kerbKeytab = context.getString("hdfs.kerberosKeytab", "");
    proxyUserName = context.getString("hdfs.proxyUser", "");

    Preconditions.checkArgument(batchSize > 0,
        "batchSize must be greater than 0");
    if (codecName == null) {
      codeC = null;
      compType = CompressionType.NONE;
    } else {
      codeC = getCodec(codecName);
      // TODO : set proper compression type
      compType = CompressionType.BLOCK;
    }

    // Do not allow user to set fileType DataStream with codeC together
    // To prevent output file with compress extension (like .snappy)
    if(fileType.equalsIgnoreCase(HDFSWriterFactory.DataStreamType)
        && codecName != null) {
      throw new IllegalArgumentException("fileType: " + fileType +
          " which does NOT support compressed output. Please don't set codeC" +
          " or change the fileType if compressed output is desired.");
    }

    if(fileType.equalsIgnoreCase(HDFSWriterFactory.CompStreamType)) {
      Preconditions.checkNotNull(codeC, "It's essential to set compress codec"
          + " when fileType is: " + fileType);
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

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
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
   * cancel the callable and throw an IOException
   */
  private <T> T callWithTimeout(Callable<T> callable)
      throws IOException, InterruptedException {
    Future<T> future = callTimeoutPool.submit(callable);
    try {
      if (callTimeout > 0) {
        return future.get(callTimeout, TimeUnit.MILLISECONDS);
      } else {
        return future.get();
      }
    } catch (TimeoutException eT) {
      future.cancel(true);
      sinkCounter.incrementConnectionFailedCount();
      throw new IOException("Callable timed out after " + callTimeout + " ms",
          eT);
    } catch (ExecutionException e1) {
      sinkCounter.incrementConnectionFailedCount();
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
   * Pull events out of channel and send it to HDFS. Take at most batchSize
   * events per Transaction. Find the corresponding bucket for the event.
   * Ensure the file is open. Serialize the data and write it to the file on
   * HDFS. <br/>
   * This method is not thread safe.
   */
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    List<BucketWriter> writers = Lists.newArrayList();
    transaction.begin();
    try {
      int txnEventCount = 0;
      for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
        Event event = channel.take();
        if (event == null) {
          break;
        }

        // reconstruct the path name by substituting place holders
        String realPath = BucketPath.escapeString(path, event.getHeaders(),
            timeZone, needRounding, roundUnit, roundValue);
        BucketWriter bucketWriter = sfWriters.get(realPath);

        // we haven't seen this file yet, so open it and cache the handle
        if (bucketWriter == null) {
          HDFSWriter hdfsWriter = writerFactory.getWriter(fileType);
          FlumeFormatter formatter = HDFSFormatterFactory
              .getFormatter(writeFormat);

          WriterCallback idleCallback = null;
          if(idleTimeout != 0) {
            idleCallback = new WriterCallback() {
              @Override
              public void run(String bucketPath) {
                sfWriters.remove(bucketPath);
              }
            };
          }
          bucketWriter = new BucketWriter(rollInterval, rollSize, rollCount,
              batchSize, context, realPath, suffix, codeC, compType, hdfsWriter,
              formatter, timedRollerPool, proxyTicket, sinkCounter, idleTimeout,
              idleCallback);

          sfWriters.put(realPath, bucketWriter);
        }

        // track the buckets getting written in this transaction
        if (!writers.contains(bucketWriter)) {
          writers.add(bucketWriter);
        }

        // Write the data to HDFS
        append(bucketWriter, event);
      }

      if (txnEventCount == 0) {
        sinkCounter.incrementBatchEmptyCount();
      } else if (txnEventCount == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      } else {
        sinkCounter.incrementBatchUnderflowCount();
      }

      // flush all pending buckets before committing the transaction
      for (BucketWriter bucketWriter : writers) {
        flush(bucketWriter);
      }

      transaction.commit();

      if (txnEventCount < 1) {
        return Status.BACKOFF;
      } else {
        sinkCounter.addToEventDrainSuccessCount(txnEventCount);
        return Status.READY;
      }
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
      transaction.close();
    }
  }

  @Override
  public void stop() {
    // do not constrain close() calls with a timeout
    for (Entry<String, BucketWriter> entry : sfWriters.entrySet()) {
      LOG.info("Closing {}", entry.getKey());

      try {
        close(entry.getValue());
      } catch (Exception ex) {
        LOG.warn("Exception while closing " + entry.getKey() + ". " +
            "Exception follows.", ex);
        if (ex instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    }

    // shut down all our thread pools
    ExecutorService toShutdown[] = { callTimeoutPool, timedRollerPool };
    for (ExecutorService execService : toShutdown) {
      execService.shutdown();
      try {
        while (execService.isTerminated() == false) {
          execService.awaitTermination(
              Math.max(defaultCallTimeout, callTimeout), TimeUnit.MILLISECONDS);
        }
      } catch (InterruptedException ex) {
        LOG.warn("shutdown interrupted on " + execService, ex);
      }
    }

    callTimeoutPool = null;
    timedRollerPool = null;

    sfWriters.clear();
    sfWriters = null;
    sinkCounter.stop();
    super.stop();
  }

  @Override
  public void start() {
    String timeoutName = "hdfs-" + getName() + "-call-runner-%d";
    callTimeoutPool = Executors.newFixedThreadPool(threadsPoolSize,
        new ThreadFactoryBuilder().setNameFormat(timeoutName).build());

    String rollerName = "hdfs-" + getName() + "-roll-timer-%d";
    timedRollerPool = Executors.newScheduledThreadPool(rollTimerPoolSize,
        new ThreadFactoryBuilder().setNameFormat(rollerName).build());

    this.sfWriters = new WriterLinkedHashMap(maxOpenFiles);
    sinkCounter.start();
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
      } else {
        //If keytab is specified, user should want it take effect.
        //HDFSEventSink will halt when keytab file is non-exist or unreadable
        File kfile = new File(kerbKeytab);
        if (!(kfile.isFile() && kfile.canRead())) {
          throw new IllegalArgumentException("The keyTab file: "
              + kerbKeytab + " is nonexistent or can't read. "
              + "Please specify a readable keytab file for Kerberos auth.");
        }
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
          kerberosLogin(this, principal, kerbKeytab);
        } catch (IOException e) {
          LOG.error("Authentication or file read error while attempting to "
              + "login as kerberos principal (" + principal + ") using "
              + "keytab (" + kerbKeytab + "). Exception follows.", e);
          return false;
        }
      } else {
        LOG.debug("{}: Using existing principal login: {}", this, curUser);
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
      HDFSEventSink sink, String principal, String keytab) throws IOException {

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

    // we already have logged in successfully
    if (curUser != null && curUser.getUserName().equals(principal)) {
      LOG.debug("{}: Using existing principal ({}): {}",
          new Object[] { sink, principal, curUser });

    // no principal found
    } else {

      LOG.info("{}: Attempting kerberos login as principal ({}) from keytab " +
          "file ({})", new Object[] { sink, principal, keytab });

      // attempt static kerberos login
      UserGroupInformation.loginUserFromKeytab(principal, keytab);
      curUser = UserGroupInformation.getLoginUser();
    }

    return curUser;
  }

  @Override
  public String toString() {
    return "{ Sink type:" + getClass().getSimpleName() + ", name:" + getName() +
        " }";
  }

  /**
   * Append to bucket writer with timeout enforced
   */
  private void append(final BucketWriter bucketWriter, final Event event)
      throws IOException, InterruptedException {

    // Write the data to HDFS
    callWithTimeout(new Callable<Void>() {
      public Void call() throws Exception {
        bucketWriter.append(event);
        return null;
      }
    });
  }

  /**
   * Flush bucket writer with timeout enforced
   */
  private void flush(final BucketWriter bucketWriter)
      throws IOException, InterruptedException {

    callWithTimeout(new Callable<Void>() {
      public Void call() throws Exception {
        bucketWriter.flush();
        return null;
      }
    });
  }

  /**
   * Close bucket writer with timeout enforced
   */
  private void close(final BucketWriter bucketWriter)
      throws IOException, InterruptedException {

    callWithTimeout(new Callable<Void>() {
      public Void call() throws Exception {
        bucketWriter.close();
        return null;
      }
    });
  }
}
