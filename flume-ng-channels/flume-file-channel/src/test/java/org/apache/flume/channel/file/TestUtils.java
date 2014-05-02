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

import static org.fest.reflect.core.Reflection.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestUtils {

  public static FlumeEvent newPersistableEvent() {
    Map<String, String> headers = Maps.newHashMap();
    String timestamp = String.valueOf(System.currentTimeMillis());
    headers.put("timestamp", timestamp);
    FlumeEvent event = new FlumeEvent(headers,
            timestamp.getBytes());
    return event;
  }

  public static FlumeEvent newPersistableEvent(int size) {
    Map<String, String> headers = Maps.newHashMap();
    String timestamp = String.valueOf(System.currentTimeMillis());
    headers.put("timestamp", timestamp);
    byte[] data = new byte[size];
    Arrays.fill(data, (byte) 54);
    FlumeEvent event = new FlumeEvent(headers, data);
    return event;
  }

  public static DataInput toDataInput(Writable writable) throws IOException {
    ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
    DataOutputStream dataOutput = new DataOutputStream(byteOutput);
    writable.write(dataOutput);
    ByteArrayInputStream byteInput = new ByteArrayInputStream(byteOutput.toByteArray());
    DataInputStream dataInput = new DataInputStream(byteInput);
    return dataInput;
  }

  public static void compareInputAndOut(Set<String> in, Set<String> out) {
    Assert.assertNotNull(in);
    Assert.assertNotNull(out);
    Assert.assertEquals(in.size(), out.size());
    Assert.assertTrue(in.equals(out));
  }

  public static Set<String> putWithoutCommit(Channel channel, Transaction tx,
          String prefix, int number) {
    Set<String> events = Sets.newHashSet();
    tx.begin();
    for (int i = 0; i < number; i++) {
      String eventData = (prefix + UUID.randomUUID()).toString();
      Event event = EventBuilder.withBody(eventData.getBytes());
      channel.put(event);
      events.add(eventData);
    }
    return events;
  }

  public static Set<String> takeWithoutCommit(Channel channel, Transaction tx,
          int number) {
    Set<String> events = Sets.newHashSet();
    tx.begin();
    for (int i = 0; i < number; i++) {
      Event e = channel.take();
      if (e == null) {
        break;
      }
      events.add(new String(e.getBody()));
    }
    return events;
  }

  public static List<File> getAllLogs(File[] dataDirs) {
    List<File> result = Lists.newArrayList();
    for(File dataDir : dataDirs) {
      result.addAll(LogUtils.getLogs(dataDir));
    }
    return result;
  }

  public static void forceCheckpoint(FileChannel channel) {
    Log log = field("log")
            .ofType(Log.class)
            .in(channel)
            .get();

    Assert.assertTrue("writeCheckpoint returned false",
            method("writeCheckpoint")
            .withReturnType(Boolean.class)
            .withParameterTypes(Boolean.class)
            .in(log)
            .invoke(true));
  }

  public static Set<String> takeEvents(Channel channel, int batchSize)
    throws Exception {
    return takeEvents(channel, batchSize, false);
  }

  public static Set<String> takeEvents(Channel channel,
          int batchSize, boolean checkForCorruption) throws Exception {
    return takeEvents(channel, batchSize, Integer.MAX_VALUE, checkForCorruption);
  }

  public static Set<String> takeEvents(Channel channel,
    int batchSize, int numEvents) throws Exception {
    return takeEvents(channel, batchSize, numEvents, false);
  }

  public static Set<String> takeEvents(Channel channel,
          int batchSize, int numEvents, boolean checkForCorruption) throws
    Exception {
    Set<String> result = Sets.newHashSet();
    for (int i = 0; i < numEvents; i += batchSize) {
      Transaction transaction = channel.getTransaction();
      try {
        transaction.begin();
        for (int j = 0; j < batchSize; j++) {
          Event event;
          try {
            event = channel.take();
          } catch (ChannelException ex) {
            Throwable th = ex;
            String msg;
            if(checkForCorruption) {
              msg = "Corrupt event found. Please run File Channel";
              th = ex.getCause();
            } else {
              msg = "Take list for FileBackedTransaction, capacity";
            }
            Assert.assertTrue(th.getMessage().startsWith(
                msg));
            if(checkForCorruption) {
              throw (Exception) th;
            }
            transaction.commit();
            return result;
          }
          if (event == null) {
            transaction.commit();
            return result;
          }
          result.add(new String(event.getBody(), Charsets.UTF_8));
        }
        transaction.commit();
      } catch (Throwable ex) {
        transaction.rollback();
        throw new RuntimeException(ex);
      } finally {
        transaction.close();
      }

    }
    return result;
  }

  public static Set<String> consumeChannel(Channel channel) throws Exception {
    return consumeChannel(channel, false);
  }
  public static Set<String> consumeChannel(Channel channel,
    boolean checkForCorruption) throws Exception {
    Set<String> result = Sets.newHashSet();
    int[] batchSizes = new int[] {
        1000, 100, 10, 1
    };
    for (int i = 0; i < batchSizes.length; i++) {
      while(true) {
        Set<String> batch = takeEvents(channel, batchSizes[i], checkForCorruption);
        if(batch.isEmpty()) {
          break;
        }
        result.addAll(batch);
      }
    }
    return result;
  }
  public static Set<String> fillChannel(Channel channel, String prefix)
      throws Exception {
    Set<String> result = Sets.newHashSet();
    int[] batchSizes = new int[] {
        1000, 100, 10, 1
    };
    for (int i = 0; i < batchSizes.length; i++) {
      try {
        while(true) {
          Set<String> batch = putEvents(channel, prefix, batchSizes[i],
              Integer.MAX_VALUE, true);
          if(batch.isEmpty()) {
            break;
          }
          result.addAll(batch);
        }
      } catch (ChannelException e) {
        Assert.assertTrue(("The channel has reached it's capacity. This might "
            + "be the result of a sink on the channel having too low of batch "
            + "size, a downstream system running slower than normal, or that "
            + "the channel capacity is just too low. [channel="
            + channel.getName() + "]").equals(e.getMessage())
            || e.getMessage().startsWith("Put queue for FileBackedTransaction " +
            "of capacity "));
      }
    }
    return result;
  }
  public static Set<String> putEvents(Channel channel, String prefix,
      int batchSize, int numEvents) throws Exception {
    return putEvents(channel, prefix, batchSize, numEvents, false);
  }
  public static Set<String> putEvents(Channel channel, String prefix,
          int batchSize, int numEvents, boolean untilCapacityIsReached)
              throws Exception {
    Set<String> result = Sets.newHashSet();
    for (int i = 0; i < numEvents; i += batchSize) {
      Transaction transaction = channel.getTransaction();
      transaction.begin();
      try {
        Set<String> batch = Sets.newHashSet();
        for (int j = 0; j < batchSize; j++) {
          String s = prefix + "-" + i + "-" + j + "-" + UUID.randomUUID();
          Event event = EventBuilder.withBody(s.getBytes(Charsets.UTF_8));
          channel.put(event);
          batch.add(s);
        }
        transaction.commit();
        result.addAll(batch);
      } catch (Exception ex) {
        transaction.rollback();
        if(untilCapacityIsReached && ex instanceof ChannelException &&
            ("The channel has reached it's capacity. "
                + "This might be the result of a sink on the channel having too "
                + "low of batch size, a downstream system running slower than "
                + "normal, or that the channel capacity is just too low. "
                + "[channel=" +channel.getName() + "]").
              equals(ex.getMessage())) {
          break;
        }
        throw ex;
      } finally {
        transaction.close();
      }
    }
    return result;
  }
  public static void copyDecompressed(String resource, File output)
      throws IOException {
    URL input =  Resources.getResource(resource);
    FileOutputStream fos = new FileOutputStream(output);
    GZIPInputStream gzis = new GZIPInputStream(input.openStream());
    ByteStreams.copy(gzis, fos);
    fos.close();
    gzis.close();
  }

  public static Context createFileChannelContext(String checkpointDir,
      String dataDir, String backupDir, Map<String, String> overrides) {
    Context context = new Context();
    context.put(FileChannelConfiguration.CHECKPOINT_DIR,
            checkpointDir);
    if(backupDir != null) {
      context.put(FileChannelConfiguration.BACKUP_CHECKPOINT_DIR, backupDir);
    }
    context.put(FileChannelConfiguration.DATA_DIRS, dataDir);
    context.put(FileChannelConfiguration.KEEP_ALIVE, String.valueOf(1));
    context.put(FileChannelConfiguration.CAPACITY, String.valueOf(10000));
    context.putAll(overrides);
    return context;
  }
  public static FileChannel createFileChannel(String checkpointDir,
    String dataDir, Map<String, String> overrides) {
    return createFileChannel(checkpointDir, dataDir, null, overrides);
  }

  public static FileChannel createFileChannel(String checkpointDir,
      String dataDir, String backupDir, Map<String, String> overrides) {
    FileChannel channel = new FileChannel();
    channel.setName("FileChannel-" + UUID.randomUUID());
    Context context = createFileChannelContext(checkpointDir, dataDir,
      backupDir, overrides);
    Configurables.configure(channel, context);
    return channel;
  }
  public static File writeStringToFile(File baseDir, String name,
      String text) throws IOException {
    File passwordFile = new File(baseDir, name);
    Files.write(text, passwordFile, Charsets.UTF_8);
    return passwordFile;
  }

}
