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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.flume.Event;
import org.apache.flume.client.avro.ReliableSpoolingFileEventReader.DeletePolicy;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.ConsumeOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import junit.framework.Assert;

public class TestReliableSpoolingFileEventReader {

  private static final Logger logger = LoggerFactory.getLogger(TestReliableSpoolingFileEventReader.class);

  private static final File WORK_DIR = new File(
      "target/test/work/" + TestReliableSpoolingFileEventReader.class.getSimpleName());

  private static final File TRACKER_DIR = new File(WORK_DIR,
      SpoolDirectorySourceConfigurationConstants.DEFAULT_TRACKER_DIR);

  @Before
  public void setup() throws IOException, InterruptedException {
    if (!WORK_DIR.isDirectory()) {
      Files.createParentDirs(new File(WORK_DIR, "dummy"));
    }

    // write out a few files
    for (int i = 0; i < 4; i++) {
      File fileName = new File(WORK_DIR, "file" + i);
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

  private void processEventsWithReader(ReliableEventReader reader, int nEvents) throws IOException {
    List<Event> events;
    do {
      events = reader.readEvents(nEvents);
      reader.commit();
    } while (!events.isEmpty());
  }

  /**
   * Verify if the give dir contains only the given files
   * 
   * @param dir
   *          the directory to check
   * @param files
   *          the files that should be contained in dir
   * @return true only if the dir contains exactly the same files given, false
   *         otherwise
   */
  private boolean checkLeftFilesInDir(File dir, String[] files) {

    List<File> actualFiles = listFiles(dir);
    Set<String> expectedFiles = new HashSet<String>(Arrays.asList(files));
    
    // Verify if the number of files in the dir is the expected
    if (actualFiles.size() != expectedFiles.size()) {
      return false;
    }
    
    // Then check files by name
    for (File f : actualFiles) {
      expectedFiles.remove(f.getName());
    }

    return expectedFiles.isEmpty();
  }

  @Test
  public void testIncludePattern() throws IOException {
    ReliableEventReader reader = new ReliableSpoolingFileEventReader.Builder()
        .spoolDirectory(WORK_DIR)
        .includePattern("^file2$")
        .deletePolicy(DeletePolicy.IMMEDIATE.toString())
        .build();
    
    String[] beforeFiles = { "file0", "file1", "file2", "file3", "emptylineFile" };
    Assert.assertTrue("Expected " + beforeFiles.length + " files in working dir",
        checkLeftFilesInDir(WORK_DIR, beforeFiles));

    processEventsWithReader(reader, 10);

    String[] afterFiles = { "file0", "file1", "file3", "emptylineFile" };
    Assert.assertTrue("Expected " + afterFiles.length + " files left in working dir",
        checkLeftFilesInDir(WORK_DIR, afterFiles));
    Assert.assertTrue("Expected no files left in tracker dir", checkLeftFilesInDir(TRACKER_DIR, new String[0]));
  }

  @Test
  public void testIgnorePattern() throws IOException {
    ReliableEventReader reader =
        new ReliableSpoolingFileEventReader.Builder()
            .spoolDirectory(WORK_DIR)
            .ignorePattern("^file2$")
            .deletePolicy(DeletePolicy.IMMEDIATE.toString())
            .build();

    String[] beforeFiles = { "file0", "file1", "file2", "file3", "emptylineFile" };
    Assert.assertTrue("Expected " + beforeFiles.length + " files in working dir",
        checkLeftFilesInDir(WORK_DIR, beforeFiles));

    processEventsWithReader(reader, 10);

    String[] files = { "file2" };
    Assert.assertTrue("Expected " + files.length + " files left in working dir", checkLeftFilesInDir(WORK_DIR, files));
    Assert.assertTrue("Expected no files left in tracker dir", checkLeftFilesInDir(TRACKER_DIR, new String[0]));
  }

  @Test
  public void testIncludeExcludePatternNoConflict() throws IOException {

    // Expected behavior mixing include/exclude conditions:
    // - file0, file1, file3: not deleted as matching ignore pattern and not
    // matching include pattern
    // - file2: deleted as not matching ignore pattern and matching include
    // pattern
    // - emptylineFile: not deleted as not matching ignore pattern but not
    // matching include pattern as well
    
    ReliableEventReader reader = new ReliableSpoolingFileEventReader.Builder()
        .spoolDirectory(WORK_DIR)
        .ignorePattern("^file[013]$")
        .includePattern("^file2$")
        .deletePolicy(DeletePolicy.IMMEDIATE.toString())
        .build(); 

    String[] beforeFiles = { "file0", "file1", "file2", "file3", "emptylineFile" };
    Assert.assertTrue("Expected " + beforeFiles.length + " files in working dir",
        checkLeftFilesInDir(WORK_DIR, beforeFiles));

    processEventsWithReader(reader, 10);

    String[] files = { "file0", "file1", "file3", "emptylineFile" };
    Assert.assertTrue("Expected " + files.length + " files left in working dir", checkLeftFilesInDir(WORK_DIR, files));
    Assert.assertTrue("Expected no files left in tracker dir", checkLeftFilesInDir(TRACKER_DIR, new String[0]));
  }

  @Test
  public void testIncludeExcludePatternConflict() throws IOException {

    // This test will stress what happens when both ignore and include options
    // are specified and the two patterns match at the same time.
    // Expected behavior:
    // - file2: not deleted as both include and ignore patterns match (safety
    // measure: ignore always wins on conflict)
    
    ReliableEventReader reader = new ReliableSpoolingFileEventReader.Builder()
        .spoolDirectory(WORK_DIR)
        .ignorePattern("^file2$")
        .includePattern("^file2$")
        .deletePolicy(DeletePolicy.IMMEDIATE.toString())
        .build();
    
    String[] beforeFiles = { "file0", "file1", "file2", "file3", "emptylineFile" };
    Assert.assertTrue("Expected " + beforeFiles.length + " files in working dir",
        checkLeftFilesInDir(WORK_DIR, beforeFiles));

    processEventsWithReader(reader, 10);

    String[] files = { "file0", "file1", "file2", "file3", "emptylineFile" };
    Assert.assertTrue("Expected " + files.length + " files left in working dir", checkLeftFilesInDir(WORK_DIR, files));
    Assert.assertTrue("Expected no files left in tracker dir", checkLeftFilesInDir(TRACKER_DIR, new String[0]));
  }

  @Test
  public void testRepeatedCallsWithCommitAlways() throws IOException {
    ReliableEventReader reader =
        new ReliableSpoolingFileEventReader.Builder().spoolDirectory(WORK_DIR)
                                                     .build();

    final int expectedLines = 1 + 1 + 2 + 3 + 1;
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

    ReliableEventReader reader =
        new ReliableSpoolingFileEventReader.Builder().spoolDirectory(WORK_DIR)
                                                     .trackerDirPath(trackerDirPath)
                                                     .build();

    final int expectedLines = 1 + 1 + 2 + 3 + 1;
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
    ReliableEventReader reader =
        new ReliableSpoolingFileEventReader.Builder().spoolDirectory(WORK_DIR)
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
    new ReliableSpoolingFileEventReader.Builder().spoolDirectory(WORK_DIR)
                                                 .consumeOrder(null)
                                                 .build();
  }
  
  @Test
  public void testConsumeFileRandomly() throws IOException {
    ReliableEventReader reader =
        new ReliableSpoolingFileEventReader.Builder().spoolDirectory(WORK_DIR)
                                                     .consumeOrder(ConsumeOrder.RANDOM)
                                                     .build();
    File fileName = new File(WORK_DIR, "new-file");
    FileUtils.write(fileName, "New file created in the end. Shoud be read randomly.\n");
    Set<String> actual = Sets.newHashSet();
    readEventsForFilesInDir(WORK_DIR, reader, actual);
    Set<String> expected = Sets.newHashSet();
    createExpectedFromFilesInSetup(expected);
    expected.add("");
    expected.add("New file created in the end. Shoud be read randomly.");
    Assert.assertEquals(expected, actual);    
  }

  @Test
  public void testConsumeFileRandomlyNewFile() throws Exception {
    // Atomic moves are not supported in Windows.
    if (SystemUtils.IS_OS_WINDOWS) {
      return;
    }
    final ReliableEventReader reader =
        new ReliableSpoolingFileEventReader.Builder().spoolDirectory(WORK_DIR)
                                                     .consumeOrder(ConsumeOrder.RANDOM)
                                                     .build();
    File fileName = new File(WORK_DIR, "new-file");
    FileUtils.write(fileName, "New file created in the end. Shoud be read randomly.\n");
    Set<String> expected = Sets.newHashSet();
    int totalFiles = WORK_DIR.listFiles().length;
    final Set<String> actual = Sets.newHashSet();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    final Semaphore semaphore1 = new Semaphore(0);
    final Semaphore semaphore2 = new Semaphore(0);
    Future<Void> wait = executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        readEventsForFilesInDir(WORK_DIR, reader, actual, semaphore1, semaphore2);
        return null;
      }
    });
    semaphore1.acquire();
    File finalFile = new File(WORK_DIR, "t-file");
    FileUtils.write(finalFile, "Last file");
    semaphore2.release();
    wait.get();
    int listFilesCount = ((ReliableSpoolingFileEventReader)reader).getListFilesCount();
    finalFile.delete();
    createExpectedFromFilesInSetup(expected);
    expected.add("");
    expected.add("New file created in the end. Shoud be read randomly.");
    expected.add("Last file");
    Assert.assertTrue(listFilesCount < (totalFiles + 2));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testConsumeFileOldest() throws IOException, InterruptedException {
    ReliableEventReader reader =
        new ReliableSpoolingFileEventReader.Builder().spoolDirectory(WORK_DIR)
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
  public void testConsumeFileYoungest() throws IOException, InterruptedException {
    ReliableEventReader reader =
        new ReliableSpoolingFileEventReader.Builder().spoolDirectory(WORK_DIR)
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
    ReliableEventReader reader =
        new ReliableSpoolingFileEventReader.Builder().spoolDirectory(WORK_DIR)
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
    ReliableEventReader reader =
        new ReliableSpoolingFileEventReader.Builder().spoolDirectory(WORK_DIR)
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

  @Test
  public void testZeroByteTrackerFile() throws IOException {
    String trackerDirPath =
        SpoolDirectorySourceConfigurationConstants.DEFAULT_TRACKER_DIR;
    File trackerDir = new File(WORK_DIR, trackerDirPath);
    if (!trackerDir.exists()) {
      trackerDir.mkdir();
    }
    File trackerFile = new File(trackerDir, ReliableSpoolingFileEventReader.metaFileName);
    if (trackerFile.exists()) {
      trackerFile.delete();
    }
    trackerFile.createNewFile();

    ReliableEventReader reader =
        new ReliableSpoolingFileEventReader.Builder().spoolDirectory(WORK_DIR)
                                                     .trackerDirPath(trackerDirPath)
                                                     .build();
    final int expectedLines = 1;
    int seenLines = 0;
    List<Event> events = reader.readEvents(10);
    int numEvents = events.size();
    if (numEvents > 0) {
      seenLines += numEvents;
      reader.commit();
    }
    // This line will fail, if the zero-byte tracker file has not been handled
    Assert.assertEquals(expectedLines, seenLines);
  }

  private void templateTestForLargeNumberOfFiles(ConsumeOrder order, Comparator<Long> comparator,
                                                 int N) throws IOException {
    File dir = null;
    try {
      dir = new File("target/test/work/" + this.getClass().getSimpleName() + "_large");
      Files.createParentDirs(new File(dir, "dummy"));
      ReliableEventReader reader =
          new ReliableSpoolingFileEventReader.Builder().spoolDirectory(dir)
                                                       .consumeOrder(order)
                                                       .build();
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
            Assert.assertEquals(((ArrayList<String>) expectedList).get(0), new String(e.getBody()));
            ((ArrayList<String>) expectedList).remove(0);
          }
        }
        reader.commit();
      }
    } finally {
      deleteDir(dir);
    }
  }

  private void readEventsForFilesInDir(File dir, ReliableEventReader reader,
                                       Collection<String> actual) throws IOException {
    readEventsForFilesInDir(dir, reader, actual, null, null);
  }
    
  /* Read events, one for each file in the given directory. */
  private void readEventsForFilesInDir(File dir, ReliableEventReader reader,
                                       Collection<String> actual, Semaphore semaphore1,
                                       Semaphore semaphore2) throws IOException {
    List<Event> events;
    boolean executed = false;
    for (int i = 0; i < listFiles(dir).size(); i++) {
      events = reader.readEvents(10);
      for (Event e : events) {
        actual.add(new String(e.getBody()));
      }
      reader.commit();
      try {
        if (!executed) {
          executed = true;
          if (semaphore1 != null) {
            semaphore1.release();
          }
          if (semaphore2 != null) {
            semaphore2.acquire();
          }
        }
      } catch (Exception ex) {
        throw new IOException(ex);
      }
    }
  }
  /* Create expected results out of the files created in the setup method. */
  private void createExpectedFromFilesInSetup(Collection<String> expected) {
    expected.add("");
    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < i; j++) {
        expected.add("file" + i + "line" + j);
      }
    }
  }

  private static List<File> listFiles(File dir) {
    List<File> files = Lists.newArrayList(dir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return !pathname.isDirectory();
      }
    }));
    return files;
  }
}
