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
package com.cloudera.flume.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * This is a straightforward and completely explicit hbase sink.
 * 
 * "writeBufferSize" - If provided, autoFlush for the HTable set to "false", and
 * writeBufferSize is set to its value. If not provided, by default autoFlush is
 * set to "true" (default HTable setting). This setting is valuable to boost
 * HBase write speed. The default is 2MB.
 * 
 * "writeToWal" - Determines whether WAL should be used during writing to HBase.
 * If not provided Puts are written to WAL by default This setting is valuable
 * to boost HBase write speed, but decreases reliability level. Use it if you
 * know what it does.
 * 
 * The Sink also implements method getSinkBuilders(), so it can be used as
 * Flume's extension plugin (see flume.plugin.classes property of flume-site.xml
 * config details)
 */
public class HBaseSink extends EventSink.Base {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseSink.class);
  public static final String KW_BUFFER_SIZE = "writeBufferSize";
  public static final String KW_USE_WAL = "writeToWal";
  public static final String USAGE = "usage: hbase(\"table\", \"rowkey\", "
      + "\"cf1\"," + " \"c1\", \"val1\"[,\"cf2\", \"c2\", \"val2\", ....]{, "
      + KW_BUFFER_SIZE + "=int, " + KW_USE_WAL + "=true|false})";

  // triples for what values to write
  public static class QualifierSpec {
    String colFam;
    String col;
    String value;

    QualifierSpec() {
    }

    QualifierSpec(String cf, String c, String v) {
      this.colFam = cf;
      this.col = c;
      this.value = v;
    }
  };

  final String tableName; // not escapable
  final String rowkey; // flume escapable string
  final List<QualifierSpec> spec;
  final long writeBufferSize;
  final boolean writeToWal;
  final Configuration config;

  private HTable table;

  public HBaseSink(String tableName, String rowkey, List<QualifierSpec> spec) {
    this(tableName, rowkey, spec, 0L, true, HBaseConfiguration.create());
  }

  public HBaseSink(String tableName, String rowkey, List<QualifierSpec> spec,
      long writeBufferSize, boolean writeToWal, Configuration config) {
    Preconditions.checkNotNull(tableName, "Must specify table name.");
    Preconditions.checkNotNull(spec, "Must specify cols and values to write. ");
    this.tableName = tableName;
    this.rowkey = rowkey;
    this.spec = spec;
    this.writeBufferSize = writeBufferSize;
    this.writeToWal = writeToWal;
    this.config = config;
  }

  @Override
  public void append(Event e) throws IOException {
    String rowVal = e.escapeString(rowkey);
    Put p = new Put(rowVal.getBytes());

    for (QualifierSpec q : spec) {
      String cf = q.colFam;
      String c = e.escapeString(q.col);
      String val = e.escapeString(q.value);
      p.add(cf.getBytes(), c.getBytes(), val.getBytes());
    }

    p.setWriteToWAL(writeToWal);
    table.put(p);
  }

  @Override
  synchronized public void close() throws IOException {
    if (table != null) {
      table.close(); // performs flushCommits() internally, so we are good when
                     // autoFlush=false
      table = null;
      LOG.info("HBase sink successfully closed");
    } else {
      LOG.warn("Double close of HBase sink");
    }

  }

  @Override
  synchronized public void open() throws IOException {
    if (table != null) {
      throw new IllegalStateException(
          "HTable is already initialized. Looks like sink close() hasn't been proceeded properly.");
    }
    // This instantiates an HTable object that connects you to
    // the tableName table.
    table = new HTable(config, tableName);
    if (writeBufferSize > 0) {
      table.setAutoFlush(false);
      table.setWriteBufferSize(writeBufferSize);
    }
    validateColFams(table);
    LOG.info("HBase sink successfully opened");
  }

  /**
   * Column family validity check happens in open(), so we through an
   * IOException (ideally invalid column families would be a
   * IllegalArgumentException but this exn doesn't make sense on open)
   */
  void validateColFams(HTable ht) throws IOException {
    for (QualifierSpec q : spec) {
      String cf = q.colFam;
      HColumnDescriptor hcd = ht.getTableDescriptor().getFamily(cf.getBytes());
      // TODO check hbase semantics
      if (hcd == null) {
        throw new IOException("The column familiy '" + cf
            + "' does not exist in table '" + tableName + "'");
      }
    }
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {

      @Override
      public EventSink build(Context context, String... argv) {
        // at least table, row, and one (cf,c,val)
        Preconditions.checkArgument(argv.length >= 2 + 3, USAGE);
        // guarantee table, row plus triples of (cf,c,val)
        Preconditions.checkArgument((argv.length % 3) == 2, USAGE);

        String tableName = argv[0];
        String rowKey = argv[1];

        List<QualifierSpec> spec = new ArrayList<QualifierSpec>();
        for (int i = 2; i < argv.length; i += 3) {
          QualifierSpec qs = new QualifierSpec();
          qs.colFam = argv[i];
          qs.col = argv[i + 1];
          qs.value = argv[i + 2];
          spec.add(qs);
        }

        String bufSzStr = context.getValue(KW_BUFFER_SIZE);
        String isWriteToWal = context.getValue(KW_USE_WAL);
        long bufSz = (bufSzStr == null ? 0 : Long.parseLong(bufSzStr));

        return new HBaseSink(tableName, rowKey, spec, bufSz,
            Boolean.parseBoolean(isWriteToWal), HBaseConfiguration.create());
      }
    };
  }

  @SuppressWarnings("unchecked")
  public static List<Pair<String, SinkFactory.SinkBuilder>> getSinkBuilders() {
    return Arrays.asList(new Pair<String, SinkFactory.SinkBuilder>("hbase",
        builder()));
  }
}
