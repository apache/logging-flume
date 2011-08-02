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

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.Preconditions;

/**
 * A simple distributed atomic counter using ZooKeeper.
 */
public class ZooKeeperCounter {
  static final Logger LOG = Logger.getLogger(ZooKeeperCounter.class);
  ZooKeeperService zkService;
  ZKClient client;
  String prefix;

  /**
   * Construct a new counter. If the counter does not exist in ZooKeeper, create
   * it with initial value 0.
   */
  public ZooKeeperCounter(ZooKeeperService svc, String prefix)
      throws IOException, KeeperException, InterruptedException {
    Preconditions.checkNotNull(svc, "ZooKeeperService cannot be null "
        + "in ZooKeeperMonotonicSequenceGenerator constructor");
    zkService = svc;
    this.prefix = prefix;

    client = zkService.createClient();
    client.init();
    client.ensureExists(this.prefix, Long.valueOf(0L).toString().getBytes());
  }

  /**
   * Construct a new counter. If the counter does not exist in ZooKeeper, create
   * it with initial value 0.
   */
  public ZooKeeperCounter(String name) throws IOException, KeeperException,
      InterruptedException {
    this(ZooKeeperService.get(), name);
  }

  /**
   * Reset counter to 0.
   */
  public void reset() throws KeeperException, IOException, InterruptedException {
    resetTo(0L);
  }

  /**
   * Reset the counter to the supplied value
   */
  public synchronized void resetTo(long value) throws KeeperException,
      IOException, InterruptedException {
    // -1 means update any version
    client.setData(this.prefix, Long.valueOf(value).toString().getBytes(), -1);
  }

  /**
   * Returns the next long in sequence, after incrementing the counter. Uses
   * test-and-set internally to increment the counter, and will therefore retry
   * until it succeeds, or if there is some exception.
   */
  public long incrementAndGet() throws IOException, KeeperException,
      InterruptedException {
    return incrementAndGetBy(1L);
  }

  /**
   * Returns the next long in sequence, after incrementing the counter. Uses
   * test-and-set internally to increment the counter, and will therefore retry
   * until it succeeds, or if there is some exception.
   */
  public long incrementAndGetBy(long incr) throws IOException, KeeperException,
      InterruptedException {
    Stat stat = new Stat();

    while (true) {
      byte[] lng = client.getData(this.prefix, false, stat);

      if (lng == null) {
        throw new IOException("Counter " + this.prefix + " has null data");
      }

      Long l = Long.parseLong(new String(lng));

      Long newcounter = l + incr;
      try {
        client.setData(this.prefix, newcounter.toString().getBytes(), stat
            .getVersion());
        return l;
      } catch (KeeperException.BadVersionException e) {
        LOG.debug("Retrying counter");
      }
    }
  }

  /**
   * Shuts down the client associated with this counter
   */
  public void shutdown() throws InterruptedException {
    Preconditions.checkNotNull(client,
        "Attempting to shutdown counter with null client");
    client.close();
  }
}
