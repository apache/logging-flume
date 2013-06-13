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
package org.apache.flume.channel.file;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

public class TestTransactionEventRecordV3 {

  @Test
  public void testTypes() throws IOException {
    Put put = new Put(System.currentTimeMillis(), WriteOrderOracle.next());
    Assert.assertEquals(TransactionEventRecord.Type.PUT.get(),
        put.getRecordType());

    Take take = new Take(System.currentTimeMillis(), WriteOrderOracle.next());
    Assert.assertEquals(TransactionEventRecord.Type.TAKE.get(),
        take.getRecordType());

    Rollback rollback = new Rollback(System.currentTimeMillis(),
        WriteOrderOracle.next());
    Assert.assertEquals(TransactionEventRecord.Type.ROLLBACK.get(),
        rollback.getRecordType());

    Commit commit = new Commit(System.currentTimeMillis(),
        WriteOrderOracle.next());
    Assert.assertEquals(TransactionEventRecord.Type.COMMIT.get(),
        commit.getRecordType());
  }
  @Test
  public void testPutSerialization() throws IOException, CorruptEventException {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("key", "value");
    Put in = new Put(System.currentTimeMillis(),
        WriteOrderOracle.next(),
        new FlumeEvent(headers, new byte[0]));
    Put out = (Put)TransactionEventRecord.fromByteArray(toByteArray(in));
    Assert.assertEquals(in.getClass(), out.getClass());
    Assert.assertEquals(in.getRecordType(), out.getRecordType());
    Assert.assertEquals(in.getTransactionID(), out.getTransactionID());
    Assert.assertEquals(in.getLogWriteOrderID(), out.getLogWriteOrderID());
    Assert.assertEquals(in.getEvent().getHeaders(), out.getEvent().getHeaders());
    Assert.assertEquals(headers, in.getEvent().getHeaders());
    Assert.assertEquals(headers, out.getEvent().getHeaders());
    Assert.assertTrue(Arrays.equals(in.getEvent().getBody(), out.getEvent().getBody()));
  }
  @Test
  public void testPutSerializationNullHeader() throws IOException,
    CorruptEventException {
    Put in = new Put(System.currentTimeMillis(),
        WriteOrderOracle.next(),
        new FlumeEvent(null, new byte[0]));
    Put out = (Put)TransactionEventRecord.fromByteArray(toByteArray(in));
    Assert.assertEquals(in.getClass(), out.getClass());
    Assert.assertEquals(in.getRecordType(), out.getRecordType());
    Assert.assertEquals(in.getTransactionID(), out.getTransactionID());
    Assert.assertEquals(in.getLogWriteOrderID(), out.getLogWriteOrderID());
    Assert.assertNull(in.getEvent().getHeaders());
    Assert.assertNotNull(out.getEvent().getHeaders());
    Assert.assertTrue(Arrays.equals(in.getEvent().getBody(), out.getEvent().getBody()));
  }
  @Test
  public void testTakeSerialization() throws IOException,
    CorruptEventException {
    Take in = new Take(System.currentTimeMillis(),
        WriteOrderOracle.next(), 10, 20);
    Take out = (Take)TransactionEventRecord.fromByteArray(toByteArray(in));
    Assert.assertEquals(in.getClass(), out.getClass());
    Assert.assertEquals(in.getRecordType(), out.getRecordType());
    Assert.assertEquals(in.getTransactionID(), out.getTransactionID());
    Assert.assertEquals(in.getLogWriteOrderID(), out.getLogWriteOrderID());
    Assert.assertEquals(in.getFileID(), out.getFileID());
    Assert.assertEquals(in.getOffset(), out.getOffset());
  }

  @Test
  public void testRollbackSerialization() throws IOException,
    CorruptEventException {
    Rollback in = new Rollback(System.currentTimeMillis(),
        WriteOrderOracle.next());
    Rollback out = (Rollback)TransactionEventRecord.fromByteArray(toByteArray(in));
    Assert.assertEquals(in.getClass(), out.getClass());
    Assert.assertEquals(in.getRecordType(), out.getRecordType());
    Assert.assertEquals(in.getTransactionID(), out.getTransactionID());
    Assert.assertEquals(in.getLogWriteOrderID(), out.getLogWriteOrderID());
  }

  @Test
  public void testCommitSerialization() throws IOException,
    CorruptEventException {
    Commit in = new Commit(System.currentTimeMillis(),
        WriteOrderOracle.next());
    Commit out = (Commit)TransactionEventRecord.fromByteArray(toByteArray(in));
    Assert.assertEquals(in.getClass(), out.getClass());
    Assert.assertEquals(in.getRecordType(), out.getRecordType());
    Assert.assertEquals(in.getTransactionID(), out.getTransactionID());
    Assert.assertEquals(in.getLogWriteOrderID(), out.getLogWriteOrderID());
  }

  @Test
  public void testBadType() throws IOException, CorruptEventException {
    TransactionEventRecord in = mock(TransactionEventRecord.class);
    when(in.getRecordType()).thenReturn(Short.MIN_VALUE);
    try {
      TransactionEventRecord.fromByteArray(toByteArray(in));
      Assert.fail();
    } catch(NullPointerException e) {
      Assert.assertEquals("Unknown action ffff8000", e.getMessage());
    }
  }

  private byte[] toByteArray(TransactionEventRecord record) throws IOException {
    ByteBuffer buffer = TransactionEventRecord.toByteBuffer(record);
    return buffer.array();
  }
}