/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.BackoffPolicy;
import com.cloudera.util.FixedRetryPolicy;
import com.cloudera.util.ResultRetryable;
import com.cloudera.util.RetryHarness;
import com.cloudera.util.Retryable;
import com.google.common.base.Preconditions;

/**
 * Wrapper for talking to ZooKeeper that provides a set of primitives we use
 * inside Flume. Not general purpose (yet), but useful to hide the complexities
 * of dealing with ZK's various error conditions.
 * 
 */
public class ZKClient implements Watcher {
  protected ZooKeeper zk;
  final String hosts;
  static final Logger LOG = LoggerFactory.getLogger(ZKClient.class);

  class ZKRetryHarness {
    final protected RetryHarness harness;

    public ZKRetryHarness(Retryable retry, BackoffPolicy policy) {
      harness = new RetryHarness(retry, policy, true);
    }

    /**
     * Run-time casts an exception into one of the three types that we know
     * could get thrown. It's really ugly.
     */
    protected void massageException(Exception e) throws IOException,
        KeeperException, InterruptedException {
      try {
        throw e;
      } catch (IOException i) {
        throw i;
      } catch (KeeperException k) {
        throw k;
      } catch (InterruptedException i) {
        throw i;
      } catch (Exception x) {
        throw new IOException("Unexpected exception type!", x);
      }
    }

    protected boolean attempt() throws InterruptedException, KeeperException,
        IOException {
      try {
        return harness.attempt();
      } catch (SessionExpiredException s) {        
        ZKClient.this.zk = null;
        ZKClient.this.init(ZKClient.this.initCallBack);
      } catch (Exception e) {
        massageException(e);
      }
      // Should never fall through here
      return false;
    }
  }

  class ZKRetryable<T> extends ResultRetryable<T> {
    /**
     * Takes default action for ZK exceptions. If CONNECTIONLOSS, try and
     * reconnect followed by abort. Otherwise, abort and signal failure.
     */
    protected void defaultHandleException(KeeperException k)
        throws KeeperException, IOException, InterruptedException {
      if (k.code() == Code.CONNECTIONLOSS) {
        throw k; // Continue
      } else {
        harness.doAbort();
        throw k;
      }
    }
  }

  /**
   * Creates a new client, but does not connect.
   */
  public ZKClient(String hosts) {
    this.hosts = hosts;
  }

  /**
   * Called when client successfully connects.
   */
  abstract public static class InitCallback {
    abstract public void success(ZKClient client) throws IOException;
  }

  /**
   * Establishes a connection with a ZooKeeper service.
   */
  public boolean init() throws IOException {
    return init(null);
  }

  InitCallback initCallBack;
  
  /**
   * Establishes a connection with a cluster of ZooKeeper servers. Throws an
   * IOException on failure.
   */
  public synchronized boolean init(final InitCallback initCallback)
      throws IOException {
    Preconditions.checkState(this.zk == null, "zk not null in ZKClient.init");
    initCallBack = initCallback;
    final Retryable retry = new Retryable() {
      public boolean doTry() throws Exception {
        // Wait on this latch for a connection to complete
        // It's important that every try gets its own latch
        final CountDownLatch latch = new CountDownLatch(1);
        final Watcher watcher = new Watcher() {
          public void process(WatchedEvent event) {
            // Don't down the latch if we weren't the most recent attempt
            if (event.getState() == KeeperState.SyncConnected) {
              latch.countDown();
            }
          }
        };

        zk = new ZooKeeper(hosts, 5000, watcher);

        if (!latch.await(5, TimeUnit.SECONDS)) {
          throw new IOException("Could not connect to ZooKeeper!");
        }
        if (initCallback != null) {          
          initCallback.success(ZKClient.this);
        }
        return true;
      };
    };
    RetryHarness harness = new RetryHarness(retry, new FixedRetryPolicy(3),
        true);

    try {
      return harness.attempt();
    } catch (IOException i) {
      throw i;
    } catch (Exception e) {
      throw new IOException("Unexpected exception connecting to ZK", e);
    }
  }

  /**
   * Closes the connection to the ZooKeeper service
   */
  public synchronized void close() throws InterruptedException {
    if (this.zk != null) {
      this.getZK().close();
      this.zk = null;
    }
  }

  /**
   * Will make sure that a node exists, with a given path and data. If the node
   * already exists, will not try to set the data.
   */
  public boolean ensureExists(final String path, final byte[] data)
      throws KeeperException, IOException, InterruptedException {
    Preconditions.checkArgument(zk != null);
    final FixedRetryPolicy policy = new FixedRetryPolicy(3);
    if (zk.exists(path, false) != null) {
      return true;
    }
    ZKRetryable<Boolean> retry = new ZKRetryable<Boolean>() {
      public boolean doTry() throws Exception {
        result = true;
        try {
          zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
          return true;
        } catch (KeeperException k) {
          if (k.code() == Code.NODEEXISTS) {
            // Note that we could have this situation: try to create, succeed
            // but get CONNECTIONLOSS, reconnect, try to create, return false.
            // Until ZK-22 goes in there really isn't a much better way of doing
            // this
            result = false;
            return true; // true to denote success of operation
          }
          defaultHandleException(k);
        }
        return false;
      }
    };

    ZKRetryHarness harness = new ZKRetryHarness(retry, policy);
    harness.attempt();

    return retry.getResult();
  }

  /**
   * Ensure that the given path is deleted, whether or not we did it.
   */
  public void ensureDeleted(final String path, final int version)
      throws KeeperException, IOException, InterruptedException {
    Preconditions.checkArgument(zk != null);
    final FixedRetryPolicy policy = new FixedRetryPolicy(3);
    ZKRetryable<Void> retry = new ZKRetryable<Void>() {
      public boolean doTry() throws Exception {
        try {
          zk.delete(path, version);
          return true;
        } catch (KeeperException k) {
          if (k.code() == Code.NONODE) {
            return true;
          }
          defaultHandleException(k);
        }
        return false;
      }
    };

    ZKRetryHarness harness = new ZKRetryHarness(retry, policy);
    harness.attempt();
  }

  /**
   * Returns the underlying ZooKeeper object. Use only for methods that are not
   * captured by this class.
   * 
   * Note that the returned ZooKeeper could be closed by another thread. This
   * method is not recommended for common use.
   */
  public synchronized ZooKeeper getZK() {
    Preconditions.checkState(zk != null, "zk is null in getZK");
    return zk;
  }

  /**
   * Returns the long suffix of a sequential znode
   */
  static public long extractSuffix(String prefix, String znode)
      throws NumberFormatException {
    // This pattern will match nodes created in ZK with the NODE_SEQUENTIAL
    // type. These nodes have a ten digit monotonic sequence number appended to
    // them, which we want to sort by.
    final Pattern pattern = Pattern.compile("(" + prefix
        + ")(\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d)$");
    Matcher matcher = pattern.matcher(znode);
    if (matcher.find()) {
      return Long.parseLong(matcher.group(2));
    } else {
      throw new NumberFormatException("Znode: " + znode
          + " does not have a numeric 10-digit suffix (Prefix was " + prefix
          + ")");
    }
  }

  /**
   * Returns the most recently created sequential child, just the node name. ZK
   * really should do this on the server side to save the O(n) traffic but until
   * it does, this is what you get.
   * 
   * Returns null if the node does not exist or it has no children.
   */
  public String getLastSequentialChild(final String path, final String prefix,
      final boolean watch) throws IOException, KeeperException,
      InterruptedException {
    Preconditions.checkArgument(zk != null);
    final FixedRetryPolicy policy = new FixedRetryPolicy(3);

    ZKRetryable<String> retry = new ZKRetryable<String>() {
      public boolean doTry() throws Exception {
        result = null;
        List<String> children = null;
        try {
          children = zk.getChildren(path, watch);
        } catch (KeeperException k) {
          defaultHandleException(k);
          return false;
        }
        if (children.size() == 0) {
          return true;
        }

        List<Long> suffixes = new ArrayList<Long>();
        for (String s : children) {
          try {
            suffixes.add(extractSuffix(prefix, s));
          } catch (NumberFormatException n) {
            // Don't throw the exception because there might be
            // some nodes that weren't created sequentially - just ignore.
            LOG.warn("Couldn't parse " + s, n);
          }
        }
        if (suffixes.size() == 0) {
          return true;
        }
        // Sorting is overkill, but convenient.
        Collections.sort(suffixes, Collections.reverseOrder());
        result = String.format(prefix + "%010d", suffixes.get(0));
        return true;
      }
    };

    ZKRetryHarness harness = new ZKRetryHarness(retry, policy);
    if (harness.attempt()) {
      return retry.getResult();
    }
    return null;
  }

  // Implementations of common ZK APIs
  public List<String> getChildren(final String path, final boolean watch)
      throws IOException, KeeperException, InterruptedException {
    Preconditions.checkArgument(zk != null);
    final FixedRetryPolicy policy = new FixedRetryPolicy(3);
    ZKRetryable<List<String>> retry = new ZKRetryable<List<String>>() {
      public boolean doTry() throws Exception {
        try {
          result = zk.getChildren(path, watch);
        } catch (KeeperException k) {
          defaultHandleException(k);
        }
        return true;
      }
    };

    ZKRetryHarness harness = new ZKRetryHarness(retry, policy);
    if (harness.attempt()) {
      return retry.getResult();
    }
    return Collections.emptyList();
  }

  public byte[] getData(final String path, final boolean watch, final Stat stat)
      throws IOException, KeeperException, InterruptedException {
    Preconditions.checkArgument(zk != null);
    final FixedRetryPolicy policy = new FixedRetryPolicy(3);
    ZKRetryable<byte[]> retry = new ZKRetryable<byte[]>() {
      public boolean doTry() throws Exception {
        try {
          result = zk.getData(path, watch, stat);
        } catch (KeeperException k) {
          defaultHandleException(k);
        }
        return true;
      }
    };
    ZKRetryHarness harness = new ZKRetryHarness(retry, policy);
    if (harness.attempt()) {
      return retry.getResult();
    }
    return null;
  }

  public Stat setData(final String path, final byte[] data, final int version)
      throws IOException, KeeperException, InterruptedException {
    Preconditions.checkArgument(zk != null);
    final FixedRetryPolicy policy = new FixedRetryPolicy(3);
    ZKRetryable<Stat> retry = new ZKRetryable<Stat>() {
      public boolean doTry() throws Exception {
        try {
          result = zk.setData(path, data, version);
        } catch (KeeperException k) {
          defaultHandleException(k);
        }
        return true;
      }
    };
    ZKRetryHarness harness = new ZKRetryHarness(retry, policy);
    if (harness.attempt()) {
      return retry.getResult();
    }
    return null;
  }

  public String create(final String path, final byte[] data,
      final List<ACL> acls, final CreateMode mode) throws IOException,
      KeeperException, InterruptedException {
    Preconditions.checkArgument(zk != null);
    final FixedRetryPolicy policy = new FixedRetryPolicy(3);
    ZKRetryable<String> retry = new ZKRetryable<String>() {
      public boolean doTry() throws IOException, KeeperException,
          InterruptedException {
        try {
          result = zk.create(path, data, acls, mode);
        } catch (KeeperException k) {
          defaultHandleException(k);
          return false;
        }
        return true;
      }
    };
    ZKRetryHarness harness = new ZKRetryHarness(retry, policy);

    if (harness.attempt()) {
      return retry.getResult();
    }
    throw new IOException("Failed to execute create");
  }

  public void delete(final String path, final int version) throws IOException,
      KeeperException, InterruptedException {
    Preconditions.checkArgument(zk != null);
    final FixedRetryPolicy policy = new FixedRetryPolicy(3);
    ZKRetryable<Void> retry = new ZKRetryable<Void>() {
      public boolean doTry() throws IOException, KeeperException,
          InterruptedException {
        try {
          zk.delete(path, version);
        } catch (KeeperException k) {
          defaultHandleException(k);
        }
        return true;
      }
    };
    ZKRetryHarness harness = new ZKRetryHarness(retry, policy);
    harness.attempt();
  }

  public Stat exists(final String path, final boolean watch)
      throws IOException, KeeperException, InterruptedException {
    Preconditions.checkArgument(zk != null);
    final FixedRetryPolicy policy = new FixedRetryPolicy(3);
    ZKRetryable<Stat> retry = new ZKRetryable<Stat>() {
      public boolean doTry() throws IOException, KeeperException,
          InterruptedException {
        try {
          result = zk.exists(path, watch);
        } catch (KeeperException k) {
          defaultHandleException(k);
        }
        return true;
      }
    };
    ZKRetryHarness harness = new ZKRetryHarness(retry, policy);

    if (harness.attempt()) {
      return retry.getResult();
    }
    throw new IOException("Failed to execute exists call");
  }

  @Override
  public void process(WatchedEvent event) {
    LOG.info("Got watched event " + event);
  }
}
