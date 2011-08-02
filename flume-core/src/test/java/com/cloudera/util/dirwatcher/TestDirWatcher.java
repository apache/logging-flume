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
package com.cloudera.util.dirwatcher;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;

import static org.junit.Assert.assertEquals;

/**
 * TODO(jon) Make this really use the mock clock, and make the test finish
 * faster. Currently it takes about 20s
 */
public class TestDirWatcher {
  public static final Logger LOG = LoggerFactory.getLogger(TestDirWatcher.class);
  FileFilter filt = new RegexFileFilter("foo.*bar");

  @Test
  public void testPrintPWD() {
    File f = new File(".");
    System.out.println(f.getAbsolutePath());
  }

  @Test
  public void testWatcher() throws IOException, InterruptedException {
    // Clock.setClock(new MockClock(0));
    DirChangeHandler simple = new DirChangeHandler() {
      @Override
      public void fileCreated(File f) {
        System.out.println("File was created: " + f);
      }

      @Override
      public void fileDeleted(File f) {
        System.out.println("File was deleted: " + f);
      }

    };

    File tempdir = FileUtil.mktempdir();
    System.out.println("Dir watcher dir = " + tempdir);

    DirWatcher w = new DirWatcher(tempdir, filt, 1000);
    w.addHandler(simple);
    w.start();

    List<File> temps = new ArrayList<File>();
    for (int i = 0; i < 20; i++) {

      if (!temps.isEmpty() && Math.random() > .5) {
        File f = temps.remove(0);
        f.delete();

      } else {
        File f = File.createTempFile("foo", "bar", tempdir);
        File temp = new File(tempdir, f.getName());
        BufferedWriter out = new BufferedWriter(new FileWriter(temp));
        out.write(temp.getName());
        out.close();
        temp.deleteOnExit();
        temps.add(temp);
      }
      Clock.sleep((long) (1000 * Math.random()));

    }

    w.stop();
    FileUtil.rmr(tempdir);
  }

  @Test
  public void testDeleteOnCreated() throws IOException, InterruptedException {
    DirChangeHandler simple = new DirChangeHandler() {
      @Override
      public void fileCreated(File f) {
        System.out.println("File was created: " + f);
        f.delete();
      }

      @Override
      public void fileDeleted(File f) {
        System.out.println("File was deleted: " + f);
      }

    };

    File tempdir = FileUtil.mktempdir();

    DirWatcher w = new DirWatcher(tempdir, filt, 1000);
    w.addHandler(simple);
    w.start();

    List<File> temps = new ArrayList<File>();
    for (int i = 0; i < 20; i++) {

      File temp = FileUtil.createTempFile("foo", "bar");
      BufferedWriter out = new BufferedWriter(new FileWriter(temp));
      out.write(temp.getName());
      out.close();
      temp.deleteOnExit();
      temps.add(temp);

      Clock.sleep((long) (1000 * Math.random()));

    }

    w.stop();

    FileUtil.rmr(tempdir);

  }

  @Test
  public void testChecksFilterWhenFileAdded() throws IOException {

    class DirChangeHandlerImp implements DirChangeHandler {
      public List<File> filesAdded = new LinkedList<File>();

      @Override
      public void fileCreated(File f) {
        filesAdded.add(f);
      }

      @Override
      public void fileDeleted(File f) {
      }
    }

    DirChangeHandlerImp handler = new DirChangeHandlerImp();
    File tempdir = FileUtil.mktempdir();

    DirWatcher watcher = new DirWatcher(tempdir, filt, 1000);
    watcher.addHandler(handler);

    File matchingFile = File.createTempFile("foo", "bar", tempdir);
    matchingFile.deleteOnExit();
    watcher.check();

    assertEquals(1, handler.filesAdded.size());
    assertEquals(matchingFile, handler.filesAdded.get(0));

    File notMatchingFile = File.createTempFile("something", "different", tempdir);
    notMatchingFile.deleteOnExit();
    watcher.check();

    assertEquals("File not matching regex should not have invoked fileCreated", 1, handler.filesAdded.size());
    assertEquals(matchingFile, handler.filesAdded.get(0));
  }

  @Test
  public void testChecksFilterWhenFileRemoved() throws IOException {

    class DirChangeHandlerImp implements DirChangeHandler {
      public List<File> filesRemoved = new LinkedList<File>();

      @Override
      public void fileCreated(File f) {
      }

      @Override
      public void fileDeleted(File f) {
        filesRemoved.add(f);
      }
    }

    DirChangeHandlerImp handler = new DirChangeHandlerImp();
    File tempdir = FileUtil.mktempdir();

    DirWatcher watcher = new DirWatcher(tempdir, filt, 1000);
    watcher.addHandler(handler);

    File matchingFile = File.createTempFile("foo", "bar", tempdir);
    watcher.check();

    matchingFile.delete();
    watcher.check();

    assertEquals(1, handler.filesRemoved.size());
    assertEquals(matchingFile, handler.filesRemoved.get(0));

    File notMatchingFile = File.createTempFile("something", "different", tempdir);
    watcher.check();

    notMatchingFile.delete();
    watcher.check();

    assertEquals("File not matching regex should not have invoked fileDeleted", 1, handler.filesRemoved.size());
    assertEquals(matchingFile, handler.filesRemoved.get(0));
  }
}
