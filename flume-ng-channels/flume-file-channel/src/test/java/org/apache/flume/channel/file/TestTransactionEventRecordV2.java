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

import junit.framework.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("deprecation")
public class TestTransactionEventRecordV2 {

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
  public void testPutSerialization() throws IOException {
    Put in = new Put(System.currentTimeMillis(),
        WriteOrderOracle.next(),
        new FlumeEvent(new HashMap<String, String>(), new byte[0]));
    Put out = (Put)TransactionEventRecord.fromDataInputV2(toDataInput(in));
    Assert.assertEquals(in.getClass(), out.getClass());
    Assert.assertEquals(in.getRecordType(), out.getRecordType());
    Assert.assertEquals(in.getTransactionID(), out.getTransactionID());
    Assert.assertEquals(in.getLogWriteOrderID(), out.getLogWriteOrderID());
    Assert.assertEquals(in.getEvent().getHeaders(), out.getEvent().getHeaders());
    Assert.assertTrue(Arrays.equals(in.getEvent().getBody(), out.getEvent().getBody()));
  }
  @Test
  public void testTakeSerialization() throws IOException {
    Take in = new Take(System.currentTimeMillis(),
        WriteOrderOracle.next(), 10, 20);
    Take out = (Take)TransactionEventRecord.fromDataInputV2(toDataInput(in));
    Assert.assertEquals(in.getClass(), out.getClass());
    Assert.assertEquals(in.getRecordType(), out.getRecordType());
    Assert.assertEquals(in.getTransactionID(), out.getTransactionID());
    Assert.assertEquals(in.getLogWriteOrderID(), out.getLogWriteOrderID());
    Assert.assertEquals(in.getFileID(), out.getFileID());
    Assert.assertEquals(in.getOffset(), out.getOffset());
  }

  @Test
  public void testRollbackSerialization() throws IOException {
    Rollback in = new Rollback(System.currentTimeMillis(),
        WriteOrderOracle.next());
    Rollback out = (Rollback)TransactionEventRecord.fromDataInputV2(toDataInput(in));
    Assert.assertEquals(in.getClass(), out.getClass());
    Assert.assertEquals(in.getRecordType(), out.getRecordType());
    Assert.assertEquals(in.getTransactionID(), out.getTransactionID());
    Assert.assertEquals(in.getLogWriteOrderID(), out.getLogWriteOrderID());
  }

  @Test
  public void testCommitSerialization() throws IOException {
    Commit in = new Commit(System.currentTimeMillis(),
        WriteOrderOracle.next());
    Commit out = (Commit)TransactionEventRecord.fromDataInputV2(toDataInput(in));
    Assert.assertEquals(in.getClass(), out.getClass());
    Assert.assertEquals(in.getRecordType(), out.getRecordType());
    Assert.assertEquals(in.getTransactionID(), out.getTransactionID());
    Assert.assertEquals(in.getLogWriteOrderID(), out.getLogWriteOrderID());
  }

  @Test
  public void testBadHeader() throws IOException {
    Put in = new Put(System.currentTimeMillis(),
        WriteOrderOracle.next(),
        new FlumeEvent(new HashMap<String, String>(), new byte[0]));
    try {
      TransactionEventRecord.fromDataInputV2(toDataInput(0, in));
      Assert.fail();
    } catch (IOException e) {
      Assert.assertEquals("Header 0 is not the required value: deadbeef",
          e.getMessage());
    }
  }

  @Test
  public void testBadType() throws IOException {
    TransactionEventRecord in = mock(TransactionEventRecord.class);
    when(in.getRecordType()).thenReturn(Short.MIN_VALUE);
    try {
      TransactionEventRecord.fromDataInputV2(toDataInput(in));
      Assert.fail();
    } catch (NullPointerException e) {
      Assert.assertEquals("Unknown action ffff8000", e.getMessage());
    }
  }

  private DataInput toDataInput(TransactionEventRecord record) throws IOException {
    ByteBuffer buffer = TransactionEventRecord.toByteBufferV2(record);
    ByteArrayInputStream byteInput = new ByteArrayInputStream(buffer.array());
    DataInputStream dataInput = new DataInputStream(byteInput);
    return dataInput;
  }
  private DataInput toDataInput(int header, TransactionEventRecord record) throws IOException {
    ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
    DataOutputStream dataOutput = new DataOutputStream(byteOutput);
    dataOutput.writeInt(header);
    dataOutput.writeShort(record.getRecordType());
    dataOutput.writeLong(record.getTransactionID());
    dataOutput.writeLong(record.getLogWriteOrderID());
    record.write(dataOutput);
    ByteArrayInputStream byteInput = new ByteArrayInputStream(byteOutput.toByteArray());
    DataInputStream dataInput = new DataInputStream(byteInput);
    return dataInput;
  }
}