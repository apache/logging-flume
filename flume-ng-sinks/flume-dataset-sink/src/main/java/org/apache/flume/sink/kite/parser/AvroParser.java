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

package org.apache.flume.sink.kite.parser;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.kite.NonRecoverableEventException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.flume.sink.kite.DatasetSinkConstants.*;

/**
 * An {@link EntityParser} that parses Avro serialized bytes from an event.
 * 
 * The Avro schema used to serialize the data should be set as either a URL
 * or literal in the flume.avro.schema.url or flume.avro.schema.literal event
 * headers respectively.
 */
public class AvroParser implements EntityParser<GenericRecord> {

  static Configuration conf = new Configuration();

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
   * The schema of the destination dataset.
   * 
   * Used as the reader schema during parsing.
   */
  private final Schema datasetSchema;

  /**
   * A cache of DatumReaders per schema.
   */
  private final LoadingCache<Schema, DatumReader<GenericRecord>> readers =
      CacheBuilder.newBuilder()
          .build(new CacheLoader<Schema, DatumReader<GenericRecord>>() {
            @Override
            public DatumReader<GenericRecord> load(Schema schema) {
              // must use the target dataset's schema for reading to ensure the
              // records are able to be stored using it
              return new GenericDatumReader<GenericRecord>(
                  schema, datasetSchema);
            }
          });

  /**
   * The binary decoder to reuse for event parsing.
   */
  private BinaryDecoder decoder = null;

  /**
   * Create a new AvroParser given the schema of the destination dataset.
   * 
   * @param datasetSchema The schema of the destination dataset.
   */
  private AvroParser(Schema datasetSchema) {
    this.datasetSchema = datasetSchema;
  }

  /**
   * Parse the entity from the body of the given event.
   * 
   * @param event The event to parse.
   * @param reuse If non-null, this may be reused and returned from this method.
   * @return The parsed entity as a GenericRecord.
   * @throws EventDeliveryException A recoverable error such as an error
   *                                downloading the schema from the URL has
   *                                occurred.
   * @throws NonRecoverableEventException A non-recoverable error such as an
   *                                      unparsable schema or entity has
   *                                      occurred.
   */
  @Override
  public GenericRecord parse(Event event, GenericRecord reuse)
      throws EventDeliveryException, NonRecoverableEventException {
    decoder = DecoderFactory.get().binaryDecoder(event.getBody(), decoder);

    try {
      DatumReader<GenericRecord> reader = readers.getUnchecked(schema(event));
      return reader.read(reuse, decoder);
    } catch (IOException ex) {
      throw new NonRecoverableEventException("Cannot deserialize event", ex);
    } catch (RuntimeException ex) {
      throw new NonRecoverableEventException("Cannot deserialize event", ex);
    }
  }

  /**
   * Get the schema from the event headers.
   * 
   * @param event The Flume event
   * @return The schema for the event
   * @throws EventDeliveryException A recoverable error such as an error
   *                                downloading the schema from the URL has
   *                                occurred.
   * @throws NonRecoverableEventException A non-recoverable error such as an
   *                                      unparsable schema has occurred.
   */
  private static Schema schema(Event event) throws EventDeliveryException,
      NonRecoverableEventException {
    Map<String, String> headers = event.getHeaders();
    String schemaURL = headers.get(AVRO_SCHEMA_URL_HEADER);
    try {
      if (schemaURL != null) {
        return schemasFromURL.get(schemaURL);
      } else {
        String schemaLiteral = headers.get(AVRO_SCHEMA_LITERAL_HEADER);
        if (schemaLiteral == null) {
          throw new NonRecoverableEventException("No schema in event headers."
              + " Headers must include either " + AVRO_SCHEMA_URL_HEADER
              + " or " + AVRO_SCHEMA_LITERAL_HEADER);
        }

        return schemasFromLiteral.get(schemaLiteral);
      }
    } catch (ExecutionException ex) {
      throw new EventDeliveryException("Cannot get schema", ex.getCause());
    } catch (UncheckedExecutionException ex) {
      throw new NonRecoverableEventException("Cannot parse schema",
          ex.getCause());
    }
  }

  public static class Builder implements EntityParser.Builder<GenericRecord> {

    @Override
    public EntityParser<GenericRecord> build(Schema datasetSchema, Context config) {
      return new AvroParser(datasetSchema);
    }
    
  }
}
