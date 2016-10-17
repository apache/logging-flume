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

package org.apache.flume.sink.hive;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;

import java.io.IOException;
import java.util.Collection;

/** Forwards the incoming event body to Hive unmodified
 * Sets up the delimiter and the field to column mapping
 */
public class HiveDelimitedTextSerializer implements HiveEventSerializer  {
  public static final String ALIAS = "DELIMITED";

  public static final String defaultDelimiter = ",";
  public static final String SERIALIZER_DELIMITER = "serializer.delimiter";
  public static final String SERIALIZER_FIELDNAMES = "serializer.fieldnames";
  public static final String SERIALIZER_SERDE_SEPARATOR = "serializer.serdeSeparator";

  private String delimiter;
  private String[] fieldToColMapping = null;
  private Character serdeSeparator = null;

  @Override
  public void write(TransactionBatch txnBatch, Event e)
          throws StreamingException, IOException, InterruptedException {
    txnBatch.write(e.getBody());
  }

  @Override
  public void write(TransactionBatch txnBatch, Collection<byte[]> events)
          throws StreamingException, IOException, InterruptedException {
    txnBatch.write(events);
  }


  @Override
  public RecordWriter createRecordWriter(HiveEndPoint endPoint)
      throws StreamingException, IOException, ClassNotFoundException {
    if (serdeSeparator == null) {
      return new DelimitedInputWriter(fieldToColMapping, delimiter, endPoint);
    }
    return new DelimitedInputWriter(fieldToColMapping, delimiter, endPoint, null, serdeSeparator);
  }

  @Override
  public void configure(Context context) {
    delimiter = parseDelimiterSpec(
            context.getString(SERIALIZER_DELIMITER, defaultDelimiter) );
    String fieldNames = context.getString(SERIALIZER_FIELDNAMES);
    if (fieldNames == null) {
      throw new IllegalArgumentException("serializer.fieldnames is not specified " +
              "for serializer " + this.getClass().getName() );
    }
    String serdeSeparatorStr = context.getString(SERIALIZER_SERDE_SEPARATOR);
    this.serdeSeparator = parseSerdeSeparatorSpec(serdeSeparatorStr);

    // split, but preserve empty fields (-1)
    fieldToColMapping = fieldNames.trim().split(",",-1);
  }

  // if delimiter is a double quoted like "\t", drop quotes
  private static String parseDelimiterSpec(String delimiter) {
    if (delimiter == null) {
      return null;
    }
    if (delimiter.charAt(0) == '"'  &&
        delimiter.charAt(delimiter.length() - 1) == '"') {
      return delimiter.substring(1,delimiter.length() - 1);
    }
    return delimiter;
  }

  // if delimiter is a single quoted character like '\t', drop quotes
  private static  Character parseSerdeSeparatorSpec(String separatorStr) {
    if (separatorStr == null) {
      return null;
    }
    if (separatorStr.length() == 1) {
      return separatorStr.charAt(0);
    }
    if (separatorStr.length() == 3    &&
        separatorStr.charAt(2) == '\''  &&
        separatorStr.charAt(separatorStr.length() - 1) == '\'') {
      return separatorStr.charAt(1);
    }

    throw new IllegalArgumentException("serializer.serdeSeparator spec is invalid " +
            "for " + ALIAS + " serializer " );
  }

}
