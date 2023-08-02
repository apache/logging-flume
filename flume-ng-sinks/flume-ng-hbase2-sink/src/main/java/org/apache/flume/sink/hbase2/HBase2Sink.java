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
package org.apache.flume.sink.hbase2;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * A simple sink which reads events from a channel and writes them to HBase 2.
 * The HBase configuration is picked up from the first <tt>hbase-site.xml</tt>
 * encountered in the classpath. This sink supports batch reading of
 * events from the channel, and writing them to HBase, to minimize the number
 * of flushes on the HBase tables. To use this sink, it has to be configured
 * with certain mandatory parameters:<p>
 * <tt>table: </tt> The name of the table in HBase to write to. <p>
 * <tt>columnFamily: </tt> The column family in HBase to write to.<p>
 * This sink will commit each transaction if the table's write buffer size is
 * reached or if the number of events in the current transaction reaches the
 * batch size, whichever comes first.<p>
 * Other optional parameters are:<p>
 * <tt>serializer:</tt> A class implementing {@link HBase2EventSerializer}.
 * An instance of
 * this class will be used to write out events to HBase.<p>
 * <tt>serializer.*:</tt> Passed in the configure() method to serializer
 * as an object of {@link org.apache.flume.Context}.<p>
 * <tt>batchSize: </tt>This is the batch size used by the client. This is the
 * maximum number of events the sink will commit per transaction. The default
 * batch size is 100 events.
 * <p>
 * <p>
 * <strong>Note: </strong> While this sink flushes all events in a transaction
 * to HBase in one shot, HBase does not guarantee atomic commits on multiple
 * rows. So if a subset of events in a batch are written to disk by HBase and
 * HBase fails, the flume transaction is rolled back, causing flume to write
 * all the events in the transaction all over again, which will cause
 * duplicates. The serializer is expected to take care of the handling of
 * duplicates etc. HBase also does not support batch increments, so if
 * multiple increments are returned by the serializer, then HBase failure
 * will cause them to be re-written, when HBase comes back up.
 */
public class HBase2Sink extends AbstractSink implements Configurable, BatchSizeSupported {
  private String tableName;
  private byte[] columnFamily;
  private Connection conn;
  private BufferedMutator table;
  private long batchSize;
  private final Configuration config;
  private static final Logger logger = LoggerFactory.getLogger(HBase2Sink.class);
  private HBase2EventSerializer serializer;
  private String kerberosPrincipal;
  private String kerberosKeytab;
  private boolean enableWal = true;
  private boolean batchIncrements = false;
  private SinkCounter sinkCounter;
  private PrivilegedExecutor privilegedExecutor;

  // Internal hooks used for unit testing.
  private DebugIncrementsCallback debugIncrCallback = null;

  public HBase2Sink() {
    this(HBaseConfiguration.create());
  }

  public HBase2Sink(Configuration conf) {
    this.config = conf;
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  HBase2Sink(Configuration conf, DebugIncrementsCallback cb) {
    this(conf);
    this.debugIncrCallback = cb;
  }

  @Override
  public void start() {
    Preconditions.checkArgument(table == null, "Please call stop " +
        "before calling start on an old instance.");
    try {
      privilegedExecutor =
          FlumeAuthenticationUtil.getAuthenticator(kerberosPrincipal, kerberosKeytab);
    } catch (Exception ex) {
      sinkCounter.incrementConnectionFailedCount();
      throw new FlumeException("Failed to login to HBase using "
          + "provided credentials.", ex);
    }
    try {
      conn = privilegedExecutor.execute((PrivilegedExceptionAction<Connection>) () -> {
        conn = ConnectionFactory.createConnection(config);
        return conn;
      });
      // Flush is controlled by us. This ensures that HBase changing
      // their criteria for flushing does not change how we flush.
      table = conn.getBufferedMutator(TableName.valueOf(tableName));

    } catch (Exception e) {
      sinkCounter.incrementConnectionFailedCount();
      logger.error("Could not load table, " + tableName +
          " from HBase", e);
      throw new FlumeException("Could not load table, " + tableName +
          " from HBase", e);
    }
    try {
      if (!privilegedExecutor.execute((PrivilegedExceptionAction<Boolean>) () -> {
        Table t = null;
        try {
          t = conn.getTable(TableName.valueOf(tableName));
          return t.getTableDescriptor().hasFamily(columnFamily);
        } finally {
          if (t != null) {
            t.close();
          }
        }
      })) {
        throw new IOException("Table " + tableName
            + " has no such column family " + Bytes.toString(columnFamily));
      }
    } catch (Exception e) {
      //Get getTableDescriptor also throws IOException, so catch the IOException
      //thrown above or by the getTableDescriptor() call.
      sinkCounter.incrementConnectionFailedCount();
      throw new FlumeException("Error getting column family from HBase."
          + "Please verify that the table " + tableName + " and Column Family, "
          + Bytes.toString(columnFamily) + " exists in HBase, and the"
          + " current user has permissions to access that table.", e);
    }

    super.start();
    sinkCounter.incrementConnectionCreatedCount();
    sinkCounter.start();
  }

  @Override
  public void stop() {
    try {
      if (table != null) {
        table.close();
      }
      table = null;
    } catch (IOException e) {
      throw new FlumeException("Error closing table.", e);
    }
    try {
      if (conn != null) {
        conn.close();
      }
      conn = null;
    } catch (IOException e) {
      throw new FlumeException("Error closing connection.", e);
    }
    sinkCounter.incrementConnectionClosedCount();
    sinkCounter.stop();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Context context) {
    if (!this.hasVersionAtLeast2()) {
      throw new ConfigurationException(
              "HBase major version number must be at least 2 for hbase2sink");
    }

    tableName = context.getString(HBase2SinkConfigurationConstants.CONFIG_TABLE);
    String cf = context.getString(
        HBase2SinkConfigurationConstants.CONFIG_COLUMN_FAMILY);
    batchSize = context.getLong(
        HBase2SinkConfigurationConstants.CONFIG_BATCHSIZE, 100L);
    Context serializerContext = new Context();
    //If not specified, will use HBase defaults.
    String eventSerializerType = context.getString(
            HBase2SinkConfigurationConstants.CONFIG_SERIALIZER);
    Preconditions.checkNotNull(tableName,
        "Table name cannot be empty, please specify in configuration file");
    Preconditions.checkNotNull(cf,
        "Column family cannot be empty, please specify in configuration file");
    //Check foe event serializer, if null set event serializer type
    if (eventSerializerType == null || eventSerializerType.isEmpty()) {
      eventSerializerType =
          "org.apache.flume.sink.hbase2.SimpleHBase2EventSerializer";
      logger.info("No serializer defined, Will use default");
    }
    serializerContext.putAll(context.getSubProperties(
        HBase2SinkConfigurationConstants.CONFIG_SERIALIZER_PREFIX));
    columnFamily = cf.getBytes(Charsets.UTF_8);
    try {
      Class<? extends HBase2EventSerializer> clazz =
          (Class<? extends HBase2EventSerializer>)
              Class.forName(eventSerializerType);
      serializer = clazz.newInstance();
      serializer.configure(serializerContext);
    } catch (Exception e) {
      logger.error("Could not instantiate event serializer.", e);
      Throwables.propagate(e);
    }
    kerberosKeytab = context.getString(HBase2SinkConfigurationConstants.CONFIG_KEYTAB);
    kerberosPrincipal = context.getString(HBase2SinkConfigurationConstants.CONFIG_PRINCIPAL);

    enableWal = context.getBoolean(HBase2SinkConfigurationConstants
        .CONFIG_ENABLE_WAL, HBase2SinkConfigurationConstants.DEFAULT_ENABLE_WAL);
    logger.info("The write to WAL option is set to: " + String.valueOf(enableWal));
    if (!enableWal) {
      logger.warn("HBase Sink's enableWal configuration is set to false. All " +
          "writes to HBase will have WAL disabled, and any data in the " +
          "memstore of this region in the Region Server could be lost!");
    }

    batchIncrements = context.getBoolean(
        HBase2SinkConfigurationConstants.CONFIG_COALESCE_INCREMENTS,
        HBase2SinkConfigurationConstants.DEFAULT_COALESCE_INCREMENTS);

    if (batchIncrements) {
      logger.info("Increment coalescing is enabled. Increments will be " +
          "buffered.");
    }

    String zkQuorum = context.getString(HBase2SinkConfigurationConstants
        .ZK_QUORUM);
    Integer port = null;
    /*
     * HBase allows multiple nodes in the quorum, but all need to use the
     * same client port. So get the nodes in host:port format,
     * and ignore the ports for all nodes except the first one. If no port is
     * specified, use default.
     */
    if (zkQuorum != null && !zkQuorum.isEmpty()) {
      StringBuilder zkBuilder = new StringBuilder();
      logger.info("Using ZK Quorum: " + zkQuorum);
      String[] zkHosts = zkQuorum.split(",");
      int length = zkHosts.length;
      for (int i = 0; i < length; i++) {
        String[] zkHostAndPort = zkHosts[i].split(":");
        zkBuilder.append(zkHostAndPort[0].trim());
        if (i != length - 1) {
          zkBuilder.append(",");
        } else {
          zkQuorum = zkBuilder.toString();
        }
        if (zkHostAndPort[1] == null) {
          throw new FlumeException("Expected client port for the ZK node!");
        }
        if (port == null) {
          port = Integer.parseInt(zkHostAndPort[1].trim());
        } else if (!port.equals(Integer.parseInt(zkHostAndPort[1].trim()))) {
          throw new FlumeException("All Zookeeper nodes in the quorum must " +
              "use the same client port.");
        }
      }
      if (port == null) {
        port = HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;
      }
      this.config.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
      this.config.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, port);
    }
    String hbaseZnode = context.getString(
        HBase2SinkConfigurationConstants.ZK_ZNODE_PARENT);
    if (hbaseZnode != null && !hbaseZnode.isEmpty()) {
      this.config.set(HConstants.ZOOKEEPER_ZNODE_PARENT, hbaseZnode);
    }
    sinkCounter = new SinkCounter(this.getName());
  }

  public Configuration getConfig() {
    return config;
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();
    List<Row> actions = new LinkedList<>();
    List<Increment> incs = new LinkedList<>();
    try {
      txn.begin();

      if (serializer instanceof BatchAware) {
        ((BatchAware) serializer).onBatchStart();
      }

      long i = 0;
      for (; i < batchSize; i++) {
        Event event = channel.take();
        if (event == null) {
          if (i == 0) {
            status = Status.BACKOFF;
            sinkCounter.incrementBatchEmptyCount();
          } else {
            sinkCounter.incrementBatchUnderflowCount();
          }
          break;
        } else {
          serializer.initialize(event, columnFamily);
          actions.addAll(serializer.getActions());
          incs.addAll(serializer.getIncrements());
        }
      }
      if (i == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      }
      sinkCounter.addToEventDrainAttemptCount(i);

      putEventsAndCommit(actions, incs, txn);

    } catch (Throwable e) {
      try {
        txn.rollback();
      } catch (Exception e2) {
        logger.error("Exception in rollback. Rollback might not have been " +
            "successful.", e2);
      }
      logger.error("Failed to commit transaction." +
          "Transaction rolled back.", e);
      sinkCounter.incrementEventWriteOrChannelFail(e);
      if (e instanceof Error || e instanceof RuntimeException) {
        logger.error("Failed to commit transaction." +
            "Transaction rolled back.", e);
        Throwables.propagate(e);
      } else {
        logger.error("Failed to commit transaction." +
            "Transaction rolled back.", e);
        throw new EventDeliveryException("Failed to commit transaction." +
            "Transaction rolled back.", e);
      }
    } finally {
      txn.close();
    }
    return status;
  }

  private void putEventsAndCommit(final List<Row> actions,
                                  final List<Increment> incs, Transaction txn) throws Exception {

    privilegedExecutor.execute((PrivilegedExceptionAction<Void>) () -> {
      final List<Mutation> mutations = new ArrayList<>(actions.size());
      for (Row r : actions) {
        if (r instanceof Put) {
          ((Put) r).setDurability(enableWal ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
        }
        // Newer versions of HBase - Increment implements Row.
        if (r instanceof Increment) {
          ((Increment) r).setDurability(enableWal ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
        }
        if (r instanceof Mutation) {
          mutations.add((Mutation)r);
        } else {
          logger.warn("dropping row " + r + " since it is not an Increment or Put");
        }
      }
      table.mutate(mutations);
      table.flush();
      return null;
    });

    privilegedExecutor.execute((PrivilegedExceptionAction<Void>) () -> {

      List<Increment> processedIncrements;
      if (batchIncrements) {
        processedIncrements = coalesceIncrements(incs);
      } else {
        processedIncrements = incs;
      }

      // Only used for unit testing.
      if (debugIncrCallback != null) {
        debugIncrCallback.onAfterCoalesce(processedIncrements);
      }

      for (final Increment i : processedIncrements) {
        i.setDurability(enableWal ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
        table.mutate(i);
      }
      table.flush();
      return null;
    });

    txn.commit();
    sinkCounter.addToEventDrainSuccessCount(actions.size());
  }


  @SuppressWarnings("unchecked")
  private Map<byte[], NavigableMap<byte[], Long>> getFamilyMap(Increment inc) {
    Preconditions.checkNotNull(inc, "Increment required");
    return inc.getFamilyMapOfLongs();
  }

  /**
   * Perform "compression" on the given set of increments so that Flume sends
   * the minimum possible number of RPC operations to HBase per batch.
   *
   * @param incs Input: Increment objects to coalesce.
   * @return List of new Increment objects after coalescing the unique counts.
   */
  private List<Increment> coalesceIncrements(Iterable<Increment> incs) {
    Preconditions.checkNotNull(incs, "List of Increments must not be null");
    // Aggregate all of the increment row/family/column counts.
    // The nested map is keyed like this: {row, family, qualifier} => count.
    Map<byte[], Map<byte[], NavigableMap<byte[], Long>>> counters = 
        Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Increment inc : incs) {
      byte[] row = inc.getRow();
      Map<byte[], NavigableMap<byte[], Long>> families = getFamilyMap(inc);
      for (Map.Entry<byte[], NavigableMap<byte[], Long>> familyEntry : families.entrySet()) {
        byte[] family = familyEntry.getKey();
        NavigableMap<byte[], Long> qualifiers = familyEntry.getValue();
        for (Map.Entry<byte[], Long> qualifierEntry : qualifiers.entrySet()) {
          byte[] qualifier = qualifierEntry.getKey();
          Long count = qualifierEntry.getValue();
          incrementCounter(counters, row, family, qualifier, count);
        }
      }
    }

    // Reconstruct list of Increments per unique row/family/qualifier.
    List<Increment> coalesced = Lists.newLinkedList();
    for (Map.Entry<byte[], Map<byte[], NavigableMap<byte[], Long>>> rowEntry :
         counters.entrySet()) {
      byte[] row = rowEntry.getKey();
      Map<byte[], NavigableMap<byte[], Long>> families = rowEntry.getValue();
      Increment inc = new Increment(row);
      for (Map.Entry<byte[], NavigableMap<byte[], Long>> familyEntry : families.entrySet()) {
        byte[] family = familyEntry.getKey();
        NavigableMap<byte[], Long> qualifiers = familyEntry.getValue();
        for (Map.Entry<byte[], Long> qualifierEntry : qualifiers.entrySet()) {
          byte[] qualifier = qualifierEntry.getKey();
          long count = qualifierEntry.getValue();
          inc.addColumn(family, qualifier, count);
        }
      }
      coalesced.add(inc);
    }

    return coalesced;
  }

  /**
   * Helper function for {@link #coalesceIncrements} to increment a counter
   * value in the passed data structure.
   *
   * @param counters  Nested data structure containing the counters.
   * @param row       Row key to increment.
   * @param family    Column family to increment.
   * @param qualifier Column qualifier to increment.
   * @param count     Amount to increment by.
   */
  private void incrementCounter(
      Map<byte[], Map<byte[], NavigableMap<byte[], Long>>> counters,
      byte[] row, byte[] family, byte[] qualifier, Long count) {

    Map<byte[], NavigableMap<byte[], Long>> families =
            counters.computeIfAbsent(row, k -> Maps.newTreeMap(Bytes.BYTES_COMPARATOR));

    NavigableMap<byte[], Long> qualifiers =
            families.computeIfAbsent(family, k -> Maps.newTreeMap(Bytes.BYTES_COMPARATOR));

    qualifiers.merge(qualifier, count, (a, b) -> a + b);
  }

  String getHBbaseVersionString() {
    return VersionInfo.getVersion();
  }

  private int getMajorVersion(String version) throws NumberFormatException {
    return Integer.parseInt(version.split("\\.")[0]);
  }

  private boolean hasVersionAtLeast2() {
    String version = getHBbaseVersionString();
    try {
      if (this.getMajorVersion(version) >= 2) {
        return true;
      }
    } catch (NumberFormatException ex) {
      logger.error(ex.getMessage());
    }
    logger.error("Invalid HBase version for hbase2sink:" + version);
    return false;
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  HBase2EventSerializer getSerializer() {
    return serializer;
  }

  @Override
  public long getBatchSize() {
    return batchSize;
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  interface DebugIncrementsCallback {
    void onAfterCoalesce(Iterable<Increment> increments);
  }
}
