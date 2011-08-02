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

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

/**
 * This generates an HBase output sink which puts event attributes into HBase
 * record based on their names. It is similar to
 * {@link com.cloudera.flume.handlers.hbase.HBaseEventSink}, please refer to
 * README.txt for basic steps.

 * Sink has the next parameters: attr2hbase("table" [,"family"[, "writeBody"[,"attrPrefix"[,"writeBufferSize"[,"writeToWal"]]]]]).
 * "table"           - HBase table name to perform output into.
 * "sysFamily"       - Column family's name which is used to store "system" data (event's timestamp, host, priority).
 *                     In case this param is absent or ="" the sink doesn't write "system" data.
 * "writeBody"       - Indicates whether event's body should be written among other "system" data.
 *                     Default is "false" which means it should NOT be written.
 *                     In case this param is absent or ="" the sink doesn't write "body" data.
 *                     In case this param has format "column-family:qualifier" the sink writes the body to the specified column-family:qualifier
 * "attrPrefix"      - Attributes with this prefix in key will be placed into HBase table. Default value: "2hb_".
 *                     Attribute key should be in the following format: "&lt;attrPrefix&gt;&lt;columnFamily&gt;:&lt;qualifier&gt;",
 *                     e.g. "2hb_user:name" means that its value will be placed into "user" column family with "name" qualifier.
 *                     Attribute with key "&lt;attrPrefix&gt;" SHOULD contain row key for Put,
 *                     otherwise (if no row can be extracted) the event is skipped and no records are written to the HBase table.
 *                     Next table shows what gets written into HBase table depending on the attribute name and other settings (in format columnFamily:qualifier->value, "-" means nothing is written).
 *
 * <blockquote><table border=1>
 *   <tr>
 *     <th>Event's attr ("name"->"value") and Event's body -> (body -> "Val") </th>
 *     <th>attrPrefix="2hb_", sysFamily=null, writeBody="bodyfam:bodycol"</th>
 *     <th>attrPrefix="2hb_", sysFamily="sysfam", writeBody="bodyfam:bodycol"</th>
 *     <th>attrPrefix="2hb_", sysFamily="sysfam", writeBody=""</th>
 *     <th>attrPrefix="2hb_", sysFamily=null, writeBody=null</th>
 *   </tr>
 *   <tr>
 *     <td>"any"->"foo", body->"EventVal"</td>
 *     <td>bodyfam:bodycol-> EventVal</td>
 *     <td>bodyfam:bodycol-> EventVal</td>
 *     <td>-</td>
 *     <td>-</td>
 *   </tr>
 *   <tr>
 *     <td>"colfam:col"->"foo", body->""</td>
 *     <td>-</td>
 *     <td>-</td>
 *     <td>-</td>
 *     <td>-</td>
 *   </tr>
 *   <tr>
 *     <td>"2hb_any"->"foo" body->"EventVal"</td>
 *     <td>bodyfam:bodycol-> EventVal</td>
 *     <td>sysfam:any->foo, bodyfam:bodycol-> EventVal</td>
 *     <td>sysfam:any->foo</td>
 *     <td>-</td>
 *   </tr>
 *   <tr>
 *     <td>"2hb_colfam:col"->"foo", body->""</td>
 *     <td>colfam:col->foo</td>
 *     <td>colfam:col->foo</td>
 *     <td>colfam:col->foo</td>
 *     <td>colfam:col->foo</td>
 *   </tr>
 * </table></blockquote>
 *
 * "writeBufferSize" - If provided, autoFlush for the HTable set to "false", and
 * writeBufferSize is set to its value. If not provided, by default autoFlush is
 * set to "true" (default HTable setting). This setting is valuable to boost
 * HBase write speed.
 *
 * "writeToWal" - Determines whether WAL should be used
 * during writing to HBase. If not provided Puts are written to WAL by default
 * This setting is valuable to boost HBase write speed, but decreases
 * reliability level. Use it if you know what it does.
 *
 * The Sink also implements method getSinkBuilders(), so it can be used as
 * Flume's extension plugin (see flume.plugin.classes property of flume-site.xml
 * config details)
 */
public class Attr2HBaseEventSink extends EventSink.Base {
  private static final Logger LOG=LoggerFactory.getLogger(Attr2HBaseEventSink.class);
  public static final String USAGE = "usage: attr2hbase(\"table\" [,\"sysFamily\"[, \"writeBody\"[,\"attrPrefix\"[,\"writeBufferSize\"[,\"writeToWal\"]]]]])";

  private String tableName;

  /**
   * Column family name to store system data like timestamp of event, host
   */
  private byte[] systemFamilyName;
  private String attrPrefix = "2hb_";
  private long writeBufferSize = 0L;
  private boolean writeToWal = true;
  private boolean writeBody = true;
  private String bodyCol;
  private String bodyFam;

  private Configuration config;
  private HTable table;

  /**
   * Instantiates sink. See detailed explanation of parameters and their values
   * at {@link com.cloudera.flume.handlers.hbase.Attr2HBaseEventSink}
   *
   * @param tableName
   *          HBase table name to output data into
   * @param systemFamilyName
   *          name of columnFamily where to store event's system data
   * @param writeBody
   *          Indicates whether event's body should be written to the specified column-family:qualifier
   * @param bodyFam
   *          indicates the column-family for writing the body
   * @param bodyCol
   *          indicates the column-qualifier for writing the body
   * @param attrPrefix
   *          attributes with this prefix in key will be placed into HBase table
   * @param writeBufferSize
   *          HTable's writeBufferSize
   * @param writeToWal
   *          determines whether WAL should be used during writing to HBase
   */
  public Attr2HBaseEventSink(String tableName, String systemFamilyName,
      boolean writeBody, String bodyFam, String bodyCol, String attrPrefix,
      long writeBufferSize, boolean writeToWal) {
    // You need a configuration object to tell the client where to connect.
    // When you create a HBaseConfiguration, it reads in whatever you've set
    // into your hbase-site.xml and in hbase-default.xml, as long as these can
    // be found on the CLASSPATH
    this(tableName, systemFamilyName, writeBody, bodyFam, bodyCol, attrPrefix,
        writeBufferSize, writeToWal, HBaseConfiguration.create());
  }

  /**
   * Instantiates sink. See detailed explanation of parameters and their values
   * at {@link com.cloudera.flume.handlers.hbase.Attr2HBaseEventSink}
   *
   * @param tableName
   *          HBase table name to output data into
   * @param systemFamilyName
   *          name of columnFamily where to store event's system data
   * @param writeBody
   *          Indicates whether event's body should be written to the specified column-family:qualifier
   * @param bodyFam
   *          indicates the column-family for writing the body
   * @param bodyCol
   *          indicates the column-qualifier for writing the body
   * @param attrPrefix
   *          attributes with this prefix in key will be placed into HBase table
   * @param writeBufferSize
   *          HTable's writeBufferSize
   * @param writeToWal
   *          determines whether WAL should be used during writing to HBase
   * @param config
   *          HBase configuration
   */
  public Attr2HBaseEventSink(String tableName, String systemFamilyName,
      boolean writeBody, String bodyFam, String bodyCol, String attrPrefix,
      long writeBufferSize, boolean writeToWal, Configuration config) {
    Preconditions.checkNotNull(tableName,
        "HBase table's name MUST be provided.");
    this.tableName = tableName;
    // systemFamilyName can be null or empty String, which means
    // "don't store "system" data
    if (systemFamilyName != null && !"".equals(systemFamilyName)) {
      this.systemFamilyName = Bytes.toBytes(systemFamilyName);
    }
    if (attrPrefix != null) {
      this.attrPrefix = attrPrefix;
    }

    this.writeBody = writeBody;
    this.bodyFam = bodyFam;
    this.bodyCol = bodyCol;

    this.writeBufferSize = writeBufferSize;
    this.writeToWal = writeToWal;
    this.config = config;
  }

  @Override
  public void append(Event e) throws IOException {
    Put p = createPut(e);

    if (p != null && p.getFamilyMap().size() > 0) {
      p.setWriteToWAL(writeToWal);
      table.put(p);
    }
  }

  // Made as package-private for unit-testing
  Put createPut(Event e) {
    Put p;
    // Attribute with key "<attrPrefix>" contains row key for Put
    if (e.getAttrs().containsKey(attrPrefix)) {
      p = new Put(e.getAttrs().get(attrPrefix));
    } else {
      LOG.warn("Cannot extract key for HBase row, the attribute with key '"
          + attrPrefix + "' is not present in event's data. No rows inserted.");
      return null;
    }

    if (systemFamilyName != null) {
      //TODO (dani) check if systemFamilyName exists in table-name
      p.add(systemFamilyName, Bytes.toBytes("timestamp"), Bytes.toBytes(e.getTimestamp()));
      p.add(systemFamilyName, Bytes.toBytes("host"), Bytes.toBytes(e.getHost()));
      if (e.getPriority() != null) {
        p.add(systemFamilyName, Bytes.toBytes("priority"), Bytes.toBytes(e.getPriority().toString()));
      }

      // Empty events are created with ""
      if (writeBody && e.getBody().length != 0) {
      //TODO (dani) check if bodyFam exists in table-name
        Put re = p.add(Bytes.toBytes(bodyFam), Bytes.toBytes(bodyCol), e.getBody());
      } else {
        LOG.warn("Skipping Body");
      }
    }

    for (Entry<String, byte[]> a : e.getAttrs().entrySet()) {
      attemptToAddAttribute(p, a);
    }
    return p;
  }

  // Made as package-private for unit-testing
  // Entry here represents event's attribute: key is attribute name and value is
  // attribute value
  void attemptToAddAttribute(Put p, Entry<String, byte[]> a) {
    String attrKey = a.getKey();
    if (attrKey.startsWith(attrPrefix)
        && attrKey.length() > attrPrefix.length()) {
      String keyWithoutPrefix = attrKey.substring(attrPrefix.length());
      // please see the javadoc of attrPrefix format for more info
      String[] col = keyWithoutPrefix.split(":", 2);
      // if both columnFamily and qualifier can be fetched from attribute's key
      boolean hasColumnFamilyAndQualifier = col.length == 2
          && col[0].length() > 0 && col[1].length() > 0;
      if (hasColumnFamilyAndQualifier) {
        p.add(Bytes.toBytes(col[0]), Bytes.toBytes(col[1]), a.getValue());
        return;
      } else if (systemFamilyName != null) {
        p.add(systemFamilyName, Bytes.toBytes(keyWithoutPrefix), a.getValue());
        return;
      } else {
        LOG.warn("Cannot determine column family and/or qualifier for attribute, attribute name: "
                + attrKey);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (table != null) {
      table.close(); // performs flushCommits() internally, so we are good when
                     // autoFlush=false
      table = null;
    }
  }

  @Override
  public void open() throws IOException {
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
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {

      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length >= 1, USAGE);

        // TODO: check that arguments has proper types
        String tableName = argv[0];
        String systemFamilyName = argv.length >= 2 ? argv[1] : null;
        // Body to be written in a column-name specified as source parameter, if
        // parameter is "" it translates to writeBody as False.
        // Default writeBody is false
        boolean writeBody = false;
        if(argv.length >= 3 && argv[2].length() >= 1)
        {
          writeBody = true;
        }
        String bodyFam = null;
        String bodyCol = null;
        if (writeBody) {
          String bodyParams[] = argv[2].split(":");
          if (bodyParams.length != 2) {
            // TODO (dani) make this message easier.
            throw new IllegalArgumentException(
                "Malformed writeBody param, usage: bodyColumnFamily:bodyColumnQualifier");
          }
          bodyFam = bodyParams[0];
          bodyCol = bodyParams[1];
        }
        String attrPrefix = argv.length >= 4 ? argv[3] : null;
        long bufferSize = argv.length >= 5 ? Long.valueOf(argv[4]) : 0;
        // TODO: add more sophisticated boolean conversion
        boolean writeToWal = argv.length >= 6 ? Boolean.valueOf(argv[5]
            .toLowerCase()) : true;
        return new Attr2HBaseEventSink(tableName, systemFamilyName, writeBody,
            bodyFam, bodyCol, attrPrefix, bufferSize, writeToWal);
      }
    };
  }

  public static List<Pair<String, SinkFactory.SinkBuilder>> getSinkBuilders() {
    return Arrays.asList(new Pair<String, SinkFactory.SinkBuilder>(
        "attr2hbase", builder()));
  }
}
