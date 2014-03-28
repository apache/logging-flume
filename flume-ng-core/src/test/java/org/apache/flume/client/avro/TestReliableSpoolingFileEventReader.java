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
package org.apache.flume.client.avro;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Event;
import org.apache.flume.client.avro.ReliableSpoolingFileEventReader.DeletePolicy;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.ConsumeOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class TestReliableSpoolingFileEventReader {

  private static final Logger logger = LoggerFactory.getLogger
      (TestReliableSpoolingFileEventReader.class);

  private static final File WORK_DIR = new File("target/test/work/" +
      TestReliableSpoolingFileEventReader.class.getSimpleName());

  @Before
  public void setup() throws IOException, InterruptedException {
    if (!WORK_DIR.isDirectory()) {
      Files.createParentDirs(new File(WORK_DIR, "dummy"));
    }

    // write out a few files
    for (int i = 0; i < 4; i++) {
      File fileName = new File(WORK_DIR, "file"+i);
      StringBuilder sb = new StringBuilder();

      // write as many lines as the index of the file
      for (int j = 0; j < i; j++) {
        sb.append("file" + i + "line" + j + "\n");
      }
      Files.write(sb.toString(), fileName, Charsets.UTF_8);
    }
    Thread.sleep(1500L); // make sure timestamp is later
    Files.write("\n", new File(WORK_DIR, "emptylineFile"), Charsets.UTF_8);
  }

  @After
  public void tearDown() {
    deleteDir(WORK_DIR);
  }

  private void deleteDir(File dir) {
    // delete all the files & dirs we created
    File[] files = dir.listFiles();
    for (File f : files) {
      if (f.isDirectory()) {
        File[] subDirFiles = f.listFiles();
        for (File sdf : subDirFiles) {
          if (!sdf.delete()) {
            logger.warn("Cannot delete file {}", sdf.getAbsolutePath());
          }
        }
        if (!f.delete()) {
          logger.warn("Cannot delete directory {}", f.getAbsolutePath());
        }
      } else {
        if (!f.delete()) {
          logger.warn("Cannot delete file {}", f.getAbsolutePath());
        }
      }
    }
    if (!dir.delete()) {
      logger.warn("Cannot delete work directory {}", dir.getAbsolutePath());
    }
  }

  @Test
  public void testIgnorePattern() throws IOException {
    ReliableEventReader reader = new ReliableSpoolingFileEventReader.Builder()
        .spoolDirectory(WORK_DIR)
        .ignorePattern("^file2$")
        .deletePolicy(DeletePolicy.IMMEDIATE.toString())
        .build();

    List<File> before = listFiles(WORK_DIR);
    Assert.assertEquals("Expected 5, not: " + before, 5, before.size());

    List<Event> events;
    do {
      events = reader.readEvents(10);
      reader.commit();
    } while (!events.isEmpty());

    List<File> after = listFiles(WORK_DIR);
    Assert.assertEquals("Expected 1, not: " + after, 1, after.size());
    Assert.assertEquals("file2", after.get(0).getName());
    List<File> trackerFiles = listFiles(new File(WORK_DIR,
        SpoolDirectorySourceConfigurationConstants.DEFAULT_TRACKER_DIR));
    Assert.assertEquals("Expected 0, not: " + trackerFiles, 0,
        trackerFiles.size());
  }

  @Test
  public void testRepeatedCallsWithCommitAlways() throws IOException {
    ReliableEventReader reader = new ReliableSpoolingFileEventReader.Builder()
        .spoolDirectory(WORK_DIR).build();

    final int expectedLines = 0 + 1 + 2 + 3 + 1;
    int seenLines = 0;
    for (int i = 0; i < 10; i++) {
      List<Event> events = reader.readEvents(10);
      seenLines += events.size();
      reader.commit();
    }

    Assert.assertEquals(expectedLines, seenLines);
  }

  @Test
  public void testRepeatedCallsWithCommitOnSuccess() throws IOException {
    String trackerDirPath =
        SpoolDirectorySourceConfigurationConstants.DEFAULT_TRACKER_DIR;
    File trackerDir = new File(WORK_DIR, trackerDirPath);

    ReliableEventReader reader = new ReliableSpoolingFileEventReader.Builder()
        .spoolDirectory(WORK_DIR).trackerDirPath(trackerDirPath).build();

    final int expectedLines = 0 + 1 + 2 + 3 + 1;
    int seenLines = 0;
    for (int i = 0; i < 10; i++) {
      List<Event> events = reader.readEvents(10);
      int numEvents = events.size();
      if (numEvents > 0) {
        seenLines += numEvents;
        reader.commit();

        // ensure that there are files in the trackerDir
        File[] files = trackerDir.listFiles();
        Assert.assertNotNull(files);
        Assert.assertTrue("Expected tracker files in tracker dir " + trackerDir
            .getAbsolutePath(), files.length > 0);
      }
    }

    Assert.assertEquals(expectedLines, seenLines);
  }

  @Test
  public void testFileDeletion() throws IOException {
    ReliableEventReader reader = new ReliableSpoolingFileEventReader.Builder()
        .spoolDirectory(WORK_DIR)
        .deletePolicy(DeletePolicy.IMMEDIATE.name())
        .build();

    List<File> before = listFiles(WORK_DIR);
    Assert.assertEquals("Expected 5, not: " + before, 5, before.size());

    List<Event> events;
    do {
      events = reader.readEvents(10);
      reader.commit();
    } while (!events.isEmpty());

    List<File> after = listFiles(WORK_DIR);
    Assert.assertEquals("Expected 0, not: " + after, 0, after.size());
    List<File> trackerFiles = listFiles(new File(WORK_DIR,
        SpoolDirectorySourceConfigurationConstants.DEFAULT_TRACKER_DIR));
    Assert.assertEquals("Expected 0, not: " + trackerFiles, 0,
        trackerFiles.size());
  }

  @Test(expected = NullPointerException.class)
  public void testNullConsumeOrder() throws IOException {
    new ReliableSpoolingFileEventReader.Builder()
    .spoolDirectory(WORK_DIR)
    .consumeOrder(null)
    .build();
  }
  
  @Test
  public void testConsumeFileRandomly() throws IOException {
    ReliableEventReader reader
      = new ReliableSpoolingFileEventReader.Builder()
    .spoolDirectory(WORK_DIR)
    .consumeOrder(ConsumeOrder.RANDOM)
    .build();
    File fileName = new File(WORK_DIR, "new-file");
    FileUtils.write(fileName,
      "New file created in the end. Shoud be read randomly.\n");
    Set<String> actual = Sets.newHashSet();
    readEventsForFilesInDir(WORK_DIR, reader, actual);      
    Set<String> expected = Sets.newHashSet();
    createExpectedFromFilesInSetup(expected);
    expected.add("");
    expected.add(
      "New file created in the end. Shoud be read randomly.");
    Assert.assertEquals(expected, actual);    
  }


  @Test
  public void testConsumeFileOldest() throws IOException, InterruptedException {
    ReliableEventReader reader
      = new ReliableSpoolingFileEventReader.Builder()
      .spoolDirectory(WORK_DIR)
      .consumeOrder(ConsumeOrder.OLDEST)
      .build();
    File file1 = new File(WORK_DIR, "new-file1");   
    File file2 = new File(WORK_DIR, "new-file2");    
    File file3 = new File(WORK_DIR, "new-file3");
    Thread.sleep(1000L);
    FileUtils.write(file2, "New file2 created.\n");
    Thread.sleep(1000L);
    FileUtils.write(file1, "New file1 created.\n");
    Thread.sleep(1000L);
    FileUtils.write(file3, "New file3 created.\n");
    // order of age oldest to youngest (file2, file1, file3)
    List<String> actual = Lists.newLinkedList();    
    readEventsForFilesInDir(WORK_DIR, reader, actual);        
    List<String> expected = Lists.newLinkedList();
    createExpectedFromFilesInSetup(expected);
    expected.add(""); // Empty file was added in the last in setup.
    expected.add("New file2 created.");
    expected.add("New file1 created.");
    expected.add("New file3 created.");    
    Assert.assertEquals(expected, actual);
  }
  
  @Test
  public void testConsumeFileYoungest()
    throws IOException, InterruptedException {
    ReliableEventReader reader
      = new ReliableSpoolingFileEventReader.Builder()
      .spoolDirectory(WORK_DIR)
      .consumeOrder(ConsumeOrder.YOUNGEST)
      .build();
    File file1 = new File(WORK_DIR, "new-file1");
    File file2 = new File(WORK_DIR, "new-file2");
    File file3 = new File(WORK_DIR, "new-file3");
    Thread.sleep(1000L);
    FileUtils.write(file2, "New file2 created.\n");
    Thread.sleep(1000L);
    FileUtils.write(file3, "New file3 created.\n");
    Thread.sleep(1000L);
    FileUtils.write(file1, "New file1 created.\n");
    // order of age youngest to oldest (file2, file3, file1)
    List<String> actual = Lists.newLinkedList();    
    readEventsForFilesInDir(WORK_DIR, reader, actual);        
    List<String> expected = Lists.newLinkedList();
    createExpectedFromFilesInSetup(expected);
    Collections.sort(expected);
    // Empty Line file was added in the last in Setup.
    expected.add(0, "");
    expected.add(0, "New file2 created.");    
    expected.add(0, "New file3 created.");
    expected.add(0, "New file1 created.");
        
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testConsumeFileOldestWithLexicographicalComparision()
    throws IOException, InterruptedException {
    ReliableEventReader reader
      = new ReliableSpoolingFileEventReader.Builder()
      .spoolDirectory(WORK_DIR)
      .consumeOrder(ConsumeOrder.OLDEST)
      .build();
    File file1 = new File(WORK_DIR, "new-file1");
    File file2 = new File(WORK_DIR, "new-file2");
    File file3 = new File(WORK_DIR, "new-file3");
    Thread.sleep(1000L);
    FileUtils.write(file3, "New file3 created.\n");
    FileUtils.write(file2, "New file2 created.\n");
    FileUtils.write(file1, "New file1 created.\n");
    file1.setLastModified(file3.lastModified());
    file1.setLastModified(file2.lastModified());
    // file ages are same now they need to be ordered
    // lexicographically (file1, file2, file3).
    List<String> actual = Lists.newLinkedList();
    readEventsForFilesInDir(WORK_DIR, reader, actual);
    List<String> expected = Lists.newLinkedList();
    createExpectedFromFilesInSetup(expected);
    expected.add(""); // Empty file was added in the last in setup.
    expected.add("New file1 created.");
    expected.add("New file2 created.");
    expected.add("New file3 created.");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testConsumeFileYoungestWithLexicographicalComparision()
    throws IOException, InterruptedException {
    ReliableEventReader reader
      = new ReliableSpoolingFileEventReader.Builder()
      .spoolDirectory(WORK_DIR)
      .consumeOrder(ConsumeOrder.YOUNGEST)
      .build();
    File file1 = new File(WORK_DIR, "new-file1");
    File file2 = new File(WORK_DIR, "new-file2");
    File file3 = new File(WORK_DIR, "new-file3");
    Thread.sleep(1000L);
    FileUtils.write(file1, "New file1 created.\n");
    FileUtils.write(file2, "New file2 created.\n");
    FileUtils.write(file3, "New file3 created.\n");
    file1.setLastModified(file3.lastModified());
    file1.setLastModified(file2.lastModified());
    // file ages are same now they need to be ordered
    // lexicographically (file1, file2, file3).
    List<String> actual = Lists.newLinkedList();
    readEventsForFilesInDir(WORK_DIR, reader, actual);
    List<String> expected = Lists.newLinkedList();
    createExpectedFromFilesInSetup(expected);
    expected.add(0, ""); // Empty file was added in the last in setup.
    expected.add(0, "New file3 created.");
    expected.add(0, "New file2 created.");
    expected.add(0, "New file1 created.");
    Assert.assertEquals(expected, actual);
  }

  @Test public void testLargeNumberOfFilesOLDEST() throws IOException {    
    templateTestForLargeNumberOfFiles(ConsumeOrder.OLDEST, null, 1000);
  }
  @Test public void testLargeNumberOfFilesYOUNGEST() throws IOException {    
    templateTestForLargeNumberOfFiles(ConsumeOrder.YOUNGEST, new Comparator<Long>() {

      @Override
      public int compare(Long o1, Long o2) {
        return o2.compareTo(o1);
      }
    }, 1000);
  }
  @Test public void testLargeNumberOfFilesRANDOM() throws IOException {    
    templateTestForLargeNumberOfFiles(ConsumeOrder.RANDOM, null, 1000);
  }
  private void templateTestForLargeNumberOfFiles(ConsumeOrder order, 
      Comparator<Long> comparator,
      int N) throws IOException {
    File dir = null;
    try {
      dir = new File(
        "target/test/work/" + this.getClass().getSimpleName() +
          "_large");
      Files.createParentDirs(new File(dir, "dummy"));
      ReliableEventReader reader
        = new ReliableSpoolingFileEventReader.Builder()
      .spoolDirectory(dir).consumeOrder(order).build();
      Map<Long, List<String>> expected;
      if (comparator == null) {
        expected = new TreeMap<Long, List<String>>();
      } else {
        expected = new TreeMap<Long, List<String>>(comparator);
      }
      for (int i = 0; i < N; i++) {
        File f = new File(dir, "file-" + i);
        String data = "file-" + i;
        Files.write(data, f, Charsets.UTF_8);
        if (expected.containsKey(f.lastModified())) {
          expected.get(f.lastModified()).add(data);
        } else {
          expected.put(f.lastModified(), Lists.newArrayList(data));
        }
      }
      Collection<String> expectedList;
      if (order == ConsumeOrder.RANDOM) {
        expectedList = Sets.newHashSet();
      } else {
        expectedList = Lists.newArrayList();
      }
      for (Entry<Long, List<String>> entry : expected.entrySet()) {
        Collections.sort(entry.getValue());
        expectedList.addAll(entry.getValue());
      }
      for (int i = 0; i < N; i++) {
        List<Event> events;
        events = reader.readEvents(10);
        for (Event e : events) {
          if (order == ConsumeOrder.RANDOM) {            
            Assert.assertTrue(expectedList.remove(new String(e.getBody())));
          } else {
            Assert.assertEquals(
              ((ArrayList<String>) expectedList).get(0),
              new String(e.getBody()));
            ((ArrayList<String>) expectedList).remove(0);
          }
        }
        reader.commit();        
      }
    } finally {
      deleteDir(dir);
    }
  }
    
  /* Read events, one for each file in the given directory. */
  private void readEventsForFilesInDir(File dir, ReliableEventReader reader, 
      Collection<String> actual) throws IOException {
    List<Event> events;
    for (int i=0; i < listFiles(dir).size(); i++) {
      events = reader.readEvents(10);
      for (Event e: events) {
        actual.add(new String(e.getBody()));
      }
      reader.commit();
    }
  }    
  /* Create expected results out of the files created in the setup method. */
  private void createExpectedFromFilesInSetup(Collection<String> expected) {
    for (int i = 0; i < 4; i++) {      
      for (int j = 0; j < i; j++) {        
        expected.add("file" + i + "line" + j);
      }      
    }
  }
  
  private static List<File> listFiles(File dir) {
    List<File> files = Lists.newArrayList(dir.listFiles(new FileFilter
        () {
      @Override
      public boolean accept(File pathname) {
        return !pathname.isDirectory();
      }
    }));
    return files;
  }
}
