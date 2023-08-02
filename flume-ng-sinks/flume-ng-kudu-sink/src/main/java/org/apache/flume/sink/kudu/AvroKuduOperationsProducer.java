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

package org.apache.flume.sink.kudu;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

/**
 * An Avro serializer that generates one operation per event by deserializing the event
 * body as an Avro record and mapping its fields to columns in a Kudu table.
 *
 * <p><strong>Avro Kudu Operations Producer configuration parameters</strong>
 * <table cellpadding=3 cellspacing=0 border=1
 *        summary="Avro Kudu Operations Producer configuration parameters">
 * <tr><th>Property Name</th>
 *   <th>Default</th>
 *   <th>Required?</th>
 *   <th>Description</th></tr>
 * <tr>
 *   <td>producer.operation</td>
 *   <td>upsert</td>
 *   <td>No</td>
 *   <td>The operation used to write events to Kudu.
 *   Supported operations are 'insert' and 'upsert'</td>
 * </tr>
 * <tr>
 *   <td>producer.schemaPath</td>
 *   <td></td>
 *   <td>No</td>
 *   <td>The location of the Avro schema file used to deserialize the Avro-encoded event bodies.
 *   It's used whenever an event does not include its own schema. If not specified, the
 *   schema must be specified on a per-event basis, either by url or as a literal.
 *   Schemas must be record type.</td>
 * </tr>
 * </table>
 */
public class AvroKuduOperationsProducer implements KuduOperationsProducer {
  public static final String OPERATION_PROP = "operation";
  public static final String SCHEMA_PROP = "schemaPath";
  public static final String DEFAULT_OPERATION = "upsert";
  public static final String SCHEMA_URL_HEADER = "flume.avro.schema.url";
  public static final String SCHEMA_LITERAL_HEADER = "flume.avro.schema.literal";

  private String operation = "";
  private GenericRecord reuse;
  private KuduTable table;
  private String defaultSchemaUrl;

  /**
   * The binary decoder to reuse for event parsing.
   */
  private BinaryDecoder decoder = null;

  /**
   * A cache of schemas retrieved by URL to avoid re-parsing the schema.
   */
  private static final LoadingCache<String, Schema> schemasFromURL =
      CacheBuilder.newBuilder()
          .build(new CacheLoader<String, Schema>() {
            @Override
            public Schema load(String url) throws IOException {
              Schema.Parser parser = new Schema.Parser();
              InputStream is = null;
              try {
                FileSystem fs = FileSystem.get(URI.create(url), conf);
                if (url.toLowerCase(Locale.ENGLISH).startsWith("hdfs:/")) {
                  is = fs.open(new Path(url));
                } else {
                  is = new URL(url).openStream();
                }
                return parser.parse(is);
              } finally {
                if (is != null) {
                  is.close();
                }
              }
            }
          });

  /**
   * A cache of literal schemas to avoid re-parsing the schema.
   */
  private static final LoadingCache<String, Schema> schemasFromLiteral =
      CacheBuilder.newBuilder()
          .build(new CacheLoader<String, Schema>() {
            @Override
            public Schema load(String literal) {
              Preconditions.checkNotNull(literal,
                  "Schema literal cannot be null without a Schema URL");
              return new Schema.Parser().parse(literal);
            }
          });

  /**
   * A cache of DatumReaders per schema.
   */
  private static final LoadingCache<Schema, DatumReader<GenericRecord>> readers =
      CacheBuilder.newBuilder()
          .build(new CacheLoader<Schema, DatumReader<GenericRecord>>() {
            @Override
            public DatumReader<GenericRecord> load(Schema schema) {
              return new GenericDatumReader<>(schema);
            }
          });

  private static final Configuration conf = new Configuration();

  public AvroKuduOperationsProducer() {
  }

  @Override
  public void configure(Context context) {
    this.operation = context.getString(OPERATION_PROP, DEFAULT_OPERATION);

    String schemaPath = context.getString(SCHEMA_PROP);
    if (schemaPath != null) {
      defaultSchemaUrl = schemaPath;
    }
  }

  @Override
  public void initialize(KuduTable table) {
    this.table = table;
  }

  @Override
  public List<Operation> getOperations(Event event) throws FlumeException {
    Schema schema = getSchema(event);
    DatumReader<GenericRecord> reader = readers.getUnchecked(schema);
    decoder = DecoderFactory.get().binaryDecoder(event.getBody(), decoder);
    try {
      reuse = reader.read(reuse, decoder);
    } catch (IOException e) {
      throw new FlumeException("Cannot deserialize event", e);
    }
    Operation op;
    switch (operation.toLowerCase(Locale.ENGLISH)) {
      case "upsert":
        op = table.newUpsert();
        break;
      case "insert":
        op = table.newInsert();
        break;
      default:
        throw new FlumeException(String.format("Unexpected operation %s", operation));
    }
    setupOp(op, reuse);
    return Collections.singletonList(op);
  }

  private void setupOp(Operation op, GenericRecord record) {
    PartialRow row = op.getRow();
    for (ColumnSchema col : table.getSchema().getColumns()) {
      String name = col.getName();
      Object value = record.get(name);
      if (value == null) {
        // Set null if nullable, otherwise leave unset for possible Kudu default.
        if (col.isNullable()) {
          row.setNull(name);
        }
      } else {
        // Avro doesn't support 8- or 16-bit integer types, but we'll allow them to be passed as
        // a larger type.
        try {
          switch (col.getType()) {
            case BOOL:
              row.addBoolean(name, (boolean) value);
              break;
            case INT8:
              row.addByte(name, (byte) value);
              break;
            case INT16:
              row.addShort(name, (short) value);
              break;
            case INT32:
              row.addInt(name, (int) value);
              break;
            case INT64: // Fall through
            case UNIXTIME_MICROS:
              row.addLong(name, (long) value);
              break;
            case FLOAT:
              row.addFloat(name, (float) value);
              break;
            case DOUBLE:
              row.addDouble(name, (double) value);
              break;
            case STRING:
              row.addString(name, value.toString());
              break;
            case BINARY:
              row.addBinary(name, (byte[]) value);
              break;
            default:
              throw new FlumeException(String.format(
                  "Unrecognized type %s for column %s", col.getType().toString(), name));
          }
        } catch (ClassCastException e) {
          throw new FlumeException(
              String.format("Failed to coerce value for column '%s' to type %s",
              col.getName(),
              col.getType()), e);
        }
      }
    }
  }

  private Schema getSchema(Event event) throws FlumeException {
    Map<String, String> headers = event.getHeaders();
    String schemaUrl = headers.get(SCHEMA_URL_HEADER);
    String schemaLiteral = headers.get(SCHEMA_LITERAL_HEADER);
    try {
      if (schemaUrl != null) {
        return schemasFromURL.get(schemaUrl);
      } else if (schemaLiteral != null) {
        return schemasFromLiteral.get(schemaLiteral);
      } else if (defaultSchemaUrl != null) {
        return schemasFromURL.get(defaultSchemaUrl);
      } else {
        throw new FlumeException(
            String.format("No schema for event. " +
                "Specify configuration property '%s' or event header '%s'",
                SCHEMA_PROP,
                SCHEMA_URL_HEADER));
      }
    } catch (ExecutionException e) {
      throw new FlumeException("Cannot get schema", e);
    } catch (RuntimeException e) {
      throw new FlumeException("Cannot parse schema", e);
    }
  }

  @Override
  public void close() {
  }
}
