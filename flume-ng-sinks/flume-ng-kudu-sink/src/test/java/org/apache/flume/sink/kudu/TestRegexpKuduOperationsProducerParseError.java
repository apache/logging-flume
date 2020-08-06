// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.flume.sink.kudu;

import static org.apache.flume.sink.kudu.RegexpKuduOperationsProducer.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.ArrayList;

import com.google.common.collect.ImmutableList;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.test.CapturingLogAppender;

public class TestRegexpKuduOperationsProducerParseError {
  private static final String TEST_REGEXP = "(?<key>\\d+),(?<byteFld>\\d+),(?<stringFld>\\w+)";
  private static final String TEST_REGEXP_MISSING_COLUMN = "(?<key>\\d+),(?<byteFld>\\d+)";
  private static final String TEST_OPERATION = "insert";

  private static final String ROW_UNMATCHING = "invalid row";
  private static final String ROW_BAD_COLUMN_VALUE = "1,1000,string";
  private static final String ROW_MISSING_COLUMN = "1,1";

  private static final String ERROR_MSG_UNMATCHED_ROW =
      "Failed to match the pattern '" + TEST_REGEXP + "' in '" + ROW_UNMATCHING + "'";
  private static final String ERROR_MSG_MISSING_COLUMN =
      "Column 'stringFld' has no matching group in '" + ROW_MISSING_COLUMN + "'";
  private static final String ERROR_MSG_BAD_COLUMN_VALUE =
      "Raw value '" + ROW_BAD_COLUMN_VALUE +
          "' couldn't be parsed to type Type: int8 for column 'byteFld'";

  private static final String POLICY_REJECT = "REJECT";
  private static final String POLICY_WARN = "WARN";
  private static final String POLICY_IGNORE = "IGNORE";

  public KuduTestHarness harness = new KuduTestHarness();
  public ExpectedException thrown = ExpectedException.none();

  // ExpectedException misbehaves when combined with other rules; we use a
  // RuleChain to beat it into submission.
  //
  // See https://stackoverflow.com/q/28846088 for more information.
  @Rule
  public RuleChain chain = RuleChain.outerRule(harness).around(thrown);

  @Test
  public void testMissingColumnThrowsExceptionDefaultConfig() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(PATTERN_PROP, TEST_REGEXP_MISSING_COLUMN);
    testThrowsException(additionalContext, ERROR_MSG_MISSING_COLUMN, ROW_MISSING_COLUMN);
  }

  @Test
  public void testMissingColumnThrowsExceptionDeprecated() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(PATTERN_PROP, TEST_REGEXP_MISSING_COLUMN);
    additionalContext.put(SKIP_MISSING_COLUMN_PROP, String.valueOf(false));
    testThrowsException(additionalContext, ERROR_MSG_MISSING_COLUMN, ROW_MISSING_COLUMN);
  }

  @Test
  public void testMissingColumnThrowsException() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(PATTERN_PROP, TEST_REGEXP_MISSING_COLUMN);
    additionalContext.put(MISSING_COLUMN_POLICY_PROP, POLICY_REJECT);
    testThrowsException(additionalContext, ERROR_MSG_MISSING_COLUMN, ROW_MISSING_COLUMN);
  }

  @Test
  public void testMissingColumnLogsWarningDeprecated() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(PATTERN_PROP, TEST_REGEXP_MISSING_COLUMN);
    additionalContext.put(SKIP_MISSING_COLUMN_PROP, String.valueOf(true));
    testLogging(additionalContext, ERROR_MSG_MISSING_COLUMN, ROW_MISSING_COLUMN);
  }

  @Test
  public void testMissingColumnLogsWarning() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(PATTERN_PROP, TEST_REGEXP_MISSING_COLUMN);
    additionalContext.put(MISSING_COLUMN_POLICY_PROP, POLICY_WARN);
    testLogging(additionalContext, ERROR_MSG_MISSING_COLUMN, ROW_MISSING_COLUMN);
  }


  @Test
  public void testMissingColumnIgnored() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(PATTERN_PROP, TEST_REGEXP_MISSING_COLUMN);
    additionalContext.put(MISSING_COLUMN_POLICY_PROP, POLICY_IGNORE);
    testIgnored(additionalContext, ERROR_MSG_MISSING_COLUMN, ROW_MISSING_COLUMN);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingColumnConfigValidation() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(SKIP_MISSING_COLUMN_PROP, String.valueOf(false));
    additionalContext.put(MISSING_COLUMN_POLICY_PROP, POLICY_IGNORE);
    getProducer(additionalContext);
  }

  @Test
  public void testBadColumnValueThrowsExceptionDefaultConfig() throws Exception {
    Context additionalContext = new Context();
    testThrowsException(additionalContext, ERROR_MSG_BAD_COLUMN_VALUE, ROW_BAD_COLUMN_VALUE);
  }

  @Test
  public void testBadColumnValueThrowsExceptionDeprecated() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(SKIP_BAD_COLUMN_VALUE_PROP, String.valueOf(false));
    testThrowsException(additionalContext, ERROR_MSG_BAD_COLUMN_VALUE, ROW_BAD_COLUMN_VALUE);
  }

  @Test
  public void testBadColumnValueThrowsException() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(BAD_COLUMN_VALUE_POLICY_PROP, POLICY_REJECT);
    testThrowsException(additionalContext, ERROR_MSG_BAD_COLUMN_VALUE, ROW_BAD_COLUMN_VALUE);
  }

  @Test
  public void testBadColumnValueLogsWarningDeprecated() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(SKIP_BAD_COLUMN_VALUE_PROP, String.valueOf(true));
    testLogging(additionalContext, ERROR_MSG_BAD_COLUMN_VALUE, ROW_BAD_COLUMN_VALUE);
  }

  @Test
  public void testBadColumnValueLogsWarning() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(BAD_COLUMN_VALUE_POLICY_PROP, POLICY_WARN);
    testLogging(additionalContext, ERROR_MSG_BAD_COLUMN_VALUE, ROW_BAD_COLUMN_VALUE);
  }

  @Test
  public void testBadColumnValueIgnored() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(BAD_COLUMN_VALUE_POLICY_PROP, POLICY_IGNORE);
    testIgnored(additionalContext, ERROR_MSG_BAD_COLUMN_VALUE, ROW_BAD_COLUMN_VALUE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadColumnValueConfigValidation() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(SKIP_BAD_COLUMN_VALUE_PROP, String.valueOf(false));
    additionalContext.put(BAD_COLUMN_VALUE_POLICY_PROP, POLICY_IGNORE);
    getProducer(additionalContext);
  }

  @Test
  public void testUnmatchedRowLogsWarningWithDefaultConfig() throws Exception {
    Context additionalContext = new Context();
    testLogging(additionalContext, ERROR_MSG_UNMATCHED_ROW, ROW_UNMATCHING);
  }

  @Test
  public void testUnmatchedRowThrowsException() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(UNMATCHED_ROW_POLICY_PROP, POLICY_REJECT);
    testThrowsException(additionalContext, ERROR_MSG_UNMATCHED_ROW, ROW_UNMATCHING);
  }

  @Test
  public void testUnmatchedRowLogsWarningDeprecated() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(WARN_UNMATCHED_ROWS_PROP, String.valueOf(true));
    testLogging(additionalContext, ERROR_MSG_UNMATCHED_ROW, ROW_UNMATCHING);
  }

  @Test
  public void testUnmatchedRowLogsWarning() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(UNMATCHED_ROW_POLICY_PROP, POLICY_WARN);
    testLogging(additionalContext, ERROR_MSG_UNMATCHED_ROW, ROW_UNMATCHING);
  }

  @Test
  public void testUnmatchedRowIgnoredDeprecated() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(WARN_UNMATCHED_ROWS_PROP, String.valueOf(false));
    testIgnored(additionalContext, ERROR_MSG_UNMATCHED_ROW, ROW_UNMATCHING);
  }

  @Test
  public void testUnmatchedRowIgnored() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(UNMATCHED_ROW_POLICY_PROP, POLICY_IGNORE);
    testIgnored(additionalContext, ERROR_MSG_UNMATCHED_ROW, ROW_UNMATCHING);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnmatchedRowConfigValidation() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(WARN_UNMATCHED_ROWS_PROP, String.valueOf(false));
    additionalContext.put(UNMATCHED_ROW_POLICY_PROP, POLICY_IGNORE);
    getProducer(additionalContext);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnKnownPolicyConfigValidation() throws Exception {
    Context additionalContext = new Context();
    additionalContext.put(UNMATCHED_ROW_POLICY_PROP, "FORCED");
    getProducer(additionalContext);
  }

  private void testLogging(
      Context additionalContext, String expectedError, String eventBody) throws Exception {
    String appendedText = processEvent(additionalContext, eventBody);
    assertTrue(appendedText.contains(expectedError));
  }

  private void testIgnored(
      Context additionalContext, String expectedError, String eventBody) throws Exception {
    String appendedText = processEvent(additionalContext, eventBody);
    assertFalse(appendedText.contains(expectedError));
  }

  private void testThrowsException(
      Context additionalContext, String expectedError, String eventBody) throws Exception {
    thrown.expect(FlumeException.class);
    thrown.expectMessage(expectedError);
    processEvent(additionalContext, eventBody);
  }

  private String processEvent(Context additionalContext, String eventBody) throws Exception {
    CapturingLogAppender appender = new CapturingLogAppender();
    RegexpKuduOperationsProducer producer = getProducer(additionalContext);
    try (Closeable c = appender.attach()) {
      producer.getOperations(EventBuilder.withBody(eventBody.getBytes(Charset.forName("UTF-8"))));
    }
    return appender.getAppendedText();
  }


  private RegexpKuduOperationsProducer getProducer(Context additionalContext) throws Exception {
    RegexpKuduOperationsProducer producer = new RegexpKuduOperationsProducer();
    producer.initialize(createNewTable("test"));
    Context context = new Context();
    context.put(PATTERN_PROP, TEST_REGEXP);
    context.put(OPERATION_PROP, TEST_OPERATION);
    context.putAll(additionalContext.getParameters());
    producer.configure(context);

    return producer;
  }

  private KuduTable createNewTable(String tableName) throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>(10);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("byteFld", Type.INT8).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("stringFld", Type.STRING).build());
    CreateTableOptions createOptions = new CreateTableOptions()
        .addHashPartitions(ImmutableList.of("key"), 3).setNumReplicas(1);
    KuduTable table =
        harness.getClient().createTable(tableName, new Schema(columns), createOptions);
    return table;
  }


}
