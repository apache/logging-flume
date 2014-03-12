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

package org.apache.flume.source;


import static org.junit.Assert.*;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Pattern;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.*;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestExecSource {

  private AbstractSource source;
  private Channel channel = new MemoryChannel();

  private Context context = new Context();

  private ChannelSelector rcs = new ReplicatingChannelSelector();


  @Before
  public void setUp() {
    context.put("keep-alive", "1");
    context.put("capacity", "1000");
    context.put("transactionCapacity", "1000");
    Configurables.configure(channel, context);
    rcs.setChannels(Lists.newArrayList(channel));

    source = new ExecSource();
    source.setChannelProcessor(new ChannelProcessor(rcs));
  }

  @After
  public void tearDown() {
    source.stop();

    // Remove the MBean registered for Monitoring
    ObjectName objName = null;
    try {
        objName = new ObjectName("org.apache.flume.source"
          + ":type=" + source.getName());

        ManagementFactory.getPlatformMBeanServer().unregisterMBean(objName);
    } catch (Exception ex) {
      System.out.println("Failed to unregister the monitored counter: "
          + objName + ex.getMessage());
    }
  }

  @Test
  public void testProcess() throws InterruptedException, LifecycleException,
  EventDeliveryException, IOException {

    // Generates a random files for input\output
    File inputFile = File.createTempFile("input", null);
    File ouputFile = File.createTempFile("ouput", null);
    FileUtils.forceDeleteOnExit(inputFile);
    FileUtils.forceDeleteOnExit(ouputFile);

    // Generates input file with a random data set (10 lines, 200 characters each)
    FileOutputStream outputStream1 = new FileOutputStream(inputFile);
    for (int i=0; i<10; i++) {
        outputStream1.write(
          RandomStringUtils.randomAlphanumeric(200).getBytes());
        outputStream1.write('\n');
    }
    outputStream1.close();

    String command = SystemUtils.IS_OS_WINDOWS ?
        String.format("cmd /c type %s", inputFile.getAbsolutePath()) :
        String.format("cat %s", inputFile.getAbsolutePath());
    context.put("command", command);
    context.put("keep-alive", "1");
    context.put("capacity", "1000");
    context.put("transactionCapacity", "1000");
    Configurables.configure(source, context);

    source.start();
    Thread.sleep(2000);
    Transaction transaction = channel.getTransaction();

    transaction.begin();
    Event event;

    FileOutputStream outputStream = new FileOutputStream(ouputFile);

    while ((event = channel.take()) != null) {
      outputStream.write(event.getBody());
      outputStream.write('\n');
    }

    outputStream.close();
    transaction.commit();
    transaction.close();

    Assert.assertEquals(FileUtils.checksumCRC32(inputFile),
      FileUtils.checksumCRC32(ouputFile));
  }

  @Test
  public void testShellCommandSimple() throws InterruptedException, LifecycleException,
    EventDeliveryException, IOException {
    if (SystemUtils.IS_OS_WINDOWS) {
      runTestShellCmdHelper("powershell -ExecutionPolicy Unrestricted -command",
        "1..5", new String[]{"1", "2", "3", "4", "5"});
    } else {
      runTestShellCmdHelper("/bin/bash -c", "seq 5",
        new String[]{"1", "2", "3", "4", "5"});
    }
  }

  @Test
  public void testShellCommandBackTicks()
    throws InterruptedException, LifecycleException, EventDeliveryException,
    IOException {
    // command with backticks
    if (SystemUtils.IS_OS_WINDOWS) {
      runTestShellCmdHelper(
        "powershell -ExecutionPolicy Unrestricted -command", "$(1..5)",
        new String[]{"1", "2", "3", "4", "5"});
    } else {
      runTestShellCmdHelper("/bin/bash -c", "echo `seq 5`",
        new String[]{"1 2 3 4 5"});
      runTestShellCmdHelper("/bin/bash -c", "echo $(seq 5)",
        new String[]{"1 2 3 4 5"});
    }
  }

  @Test
  public void testShellCommandComplex()
    throws InterruptedException, LifecycleException, EventDeliveryException,
    IOException {
    // command with wildcards & pipes
    String[] expected = {"1234", "abcd", "ijk", "xyz", "zzz"};
    // pipes
    if (SystemUtils.IS_OS_WINDOWS) {
      runTestShellCmdHelper(
        "powershell -ExecutionPolicy Unrestricted -command",
        "'zzz','1234','xyz','abcd','ijk' | sort", expected);
    } else {
      runTestShellCmdHelper("/bin/bash -c",
        "echo zzz 1234 xyz abcd ijk | xargs -n1 echo | sort -f", expected);
    }
  }

  @Test
  public void testShellCommandScript()
    throws InterruptedException, LifecycleException, EventDeliveryException,
    IOException {
    // mini script
    if (SystemUtils.IS_OS_WINDOWS) {
      runTestShellCmdHelper("powershell -ExecutionPolicy Unrestricted -command",
        "foreach ($i in 1..5) { $i }", new String[]{"1", "2", "3", "4", "5"});
      // shell arithmetic
      runTestShellCmdHelper("powershell -ExecutionPolicy Unrestricted -command",
        "if(2+2 -gt 3) { 'good' } else { 'not good' } ", new String[]{"good"});
    } else {
      runTestShellCmdHelper("/bin/bash -c", "for i in {1..5}; do echo $i;done"
        , new String[]{"1", "2", "3", "4", "5"});
      // shell arithmetic
      runTestShellCmdHelper("/bin/bash -c", "if ((2+2>3)); " +
        "then  echo good; else echo not good; fi", new String[]{"good"});
    }
  }

  @Test
  public void testShellCommandEmbeddingAndEscaping()
    throws InterruptedException, LifecycleException, EventDeliveryException,
    IOException {
    // mini script
    String fileName = SystemUtils.IS_OS_WINDOWS ?
                      "src\\test\\resources\\test_command.ps1" :
                      "src/test/resources/test_command.txt";
    BufferedReader reader = new BufferedReader(new FileReader(fileName));
      try {
        String shell = SystemUtils.IS_OS_WINDOWS ?
                       "powershell -ExecutionPolicy Unrestricted -command" :
                       "/bin/bash -c";
        String command1 = reader.readLine();
        Assert.assertNotNull(command1);
        String[] output1 = new String[] {"'1'", "\"2\"", "\\3", "\\4"};
        runTestShellCmdHelper( shell, command1 , output1);
        String command2 = reader.readLine();
        Assert.assertNotNull(command2);
        String[] output2 = new String[]{"1","2","3","4","5" };
        runTestShellCmdHelper(shell, command2 , output2);
        String command3 = reader.readLine();
        Assert.assertNotNull(command3);
        String[] output3 = new String[]{"2","3","4","5","6" };
        runTestShellCmdHelper(shell, command3 , output3);
      } finally {
        reader.close();
      }
    }

  @Test
  public void testMonitoredCounterGroup() throws InterruptedException, LifecycleException,
  EventDeliveryException, IOException {
    // mini script
    if (SystemUtils.IS_OS_WINDOWS) {
      runTestShellCmdHelper("powershell -ExecutionPolicy Unrestricted -command",
        "foreach ($i in 1..5) { $i }"
        , new String[]{"1", "2", "3", "4", "5"});
    } else {
      runTestShellCmdHelper("/bin/bash -c", "for i in {1..5}; do echo $i;done"
        , new String[]{"1", "2", "3", "4", "5"});
    }

    ObjectName objName = null;

    try {
        objName = new ObjectName("org.apache.flume.source"
          + ":type=" + source.getName());

        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        String strAtts[] = {"Type", "EventReceivedCount", "EventAcceptedCount"};
        AttributeList attrList = mbeanServer.getAttributes(objName, strAtts);

        Assert.assertNotNull(attrList.get(0));
        Assert.assertEquals("Expected Value: Type", "Type",
                ((Attribute) attrList.get(0)).getName());
        Assert.assertEquals("Expected Value: SOURCE", "SOURCE",
                ((Attribute) attrList.get(0)).getValue());

        Assert.assertNotNull(attrList.get(1));
        Assert.assertEquals("Expected Value: EventReceivedCount", "EventReceivedCount",
                ((Attribute) attrList.get(1)).getName());
        Assert.assertEquals("Expected Value: 5", "5",
                ((Attribute) attrList.get(1)).getValue().toString());

        Assert.assertNotNull(attrList.get(2));
        Assert.assertEquals("Expected Value: EventAcceptedCount", "EventAcceptedCount",
                ((Attribute) attrList.get(2)).getName());
        Assert.assertEquals("Expected Value: 5", "5",
                ((Attribute) attrList.get(2)).getValue().toString());

    } catch (Exception ex) {
      System.out.println("Unable to retreive the monitored counter: "
          + objName + ex.getMessage());
    }
  }

  @Test
  public void testBatchTimeout() throws InterruptedException, LifecycleException,
  EventDeliveryException, IOException {

    String filePath = "/tmp/flume-execsource." + Thread.currentThread().getId();
    String eventBody = "TestMessage";
    FileOutputStream outputStream = new FileOutputStream(filePath);

    context.put(ExecSourceConfigurationConstants.CONFIG_BATCH_SIZE, "50000");
    context.put(ExecSourceConfigurationConstants.CONFIG_BATCH_TIME_OUT, "750");
    context.put("shell", SystemUtils.IS_OS_WINDOWS ?
                         "powershell -ExecutionPolicy Unrestricted -command" :
                         "/bin/bash -c");
    context.put("command", SystemUtils.IS_OS_WINDOWS ?
                           "Get-Content " + filePath +
                             " | Select-Object -Last 10" :
                           ("tail -f " + filePath));

    Configurables.configure(source, context);
    source.start();

    Transaction transaction = channel.getTransaction();
    transaction.begin();

    for (int lineNumber = 0; lineNumber < 3; lineNumber++) {
        outputStream.write((eventBody).getBytes());
        outputStream.write(String.valueOf(lineNumber).getBytes());
        outputStream.write('\n');
        outputStream.flush();
    }
    outputStream.close();
    Thread.sleep(1500);

    for(int i = 0; i < 3; i++) {
      Event event = channel.take();
      assertNotNull(event);
      assertNotNull(event.getBody());
      assertEquals(eventBody + String.valueOf(i), new String(event.getBody()));
    }

    transaction.commit();
    transaction.close();
    source.stop();
    File file = new File(filePath);
    FileUtils.forceDelete(file);
  }

    private void runTestShellCmdHelper(String shell, String command, String[] expectedOutput)
             throws InterruptedException, LifecycleException, EventDeliveryException, IOException {
      context.put("shell", shell);
      context.put("command", command);
      Configurables.configure(source, context);
      source.start();
      File outputFile = File.createTempFile("flumeExecSourceTest_", "");
      FileOutputStream outputStream = new FileOutputStream(outputFile);
      if(SystemUtils.IS_OS_WINDOWS)
           Thread.sleep(2500);
      Transaction transaction = channel.getTransaction();
      transaction.begin();
      try {
        Event event;
        while ((event = channel.take()) != null) {
          outputStream.write(event.getBody());
          outputStream.write('\n');
        }
        outputStream.close();
        transaction.commit();
        List<String> output  = Files.readLines(outputFile, Charset.defaultCharset());
//        System.out.println("command : " + command);
//        System.out.println("output : ");
//        for( String line : output )
//          System.out.println(line);
        Assert.assertArrayEquals(expectedOutput, output.toArray(new String[]{}));
      } finally {
        FileUtils.forceDelete(outputFile);
        transaction.close();
        source.stop();
      }
    }


  @Test
  public void testRestart() throws InterruptedException, LifecycleException,
  EventDeliveryException, IOException {

    context.put(ExecSourceConfigurationConstants.CONFIG_RESTART_THROTTLE, "10");
    context.put(ExecSourceConfigurationConstants.CONFIG_RESTART, "true");

    context.put("command",
      SystemUtils.IS_OS_WINDOWS ? "cmd /c echo flume" : "echo flume");
    Configurables.configure(source, context);

    source.start();
    Transaction transaction = channel.getTransaction();

    transaction.begin();

    long start = System.currentTimeMillis();

    for(int i = 0; i < 5; i++) {
      Event event = channel.take();
      assertNotNull(event);
      assertNotNull(event.getBody());
      assertEquals("flume", new String(event.getBody(), Charsets.UTF_8));
    }

    // ensure restartThrottle was turned down as expected
    assertTrue(System.currentTimeMillis() - start < 10000L);

    transaction.commit();
    transaction.close();

    source.stop();
  }


  /**
   * Tests to make sure that the shutdown mechanism works. There are races
   * in this test if the system has another sleep command running with the
   * same sleep interval but we pick rarely used sleep times and make an
   * effort to detect if our sleep time is already in use. Note the
   * ps -ef command should work on both macs and linux.
   */
  @Test
  public void testShutdown() throws Exception {
    int seconds = 272; // pick a rare sleep time

    // now find one that is not in use
    boolean searchForCommand = true;
    while (searchForCommand) {
      searchForCommand = false;
      String command = SystemUtils.IS_OS_WINDOWS ? ("cmd /c sleep " + seconds) :
                       ("sleep " + seconds);
      String searchTxt = SystemUtils.IS_OS_WINDOWS ? ("sleep.exe") :
                         ("\b" + command + "\b");
      Pattern pattern = Pattern.compile(searchTxt);
      for (String line : exec(SystemUtils.IS_OS_WINDOWS ?
                              "cmd /c tasklist /FI \"SESSIONNAME eq Console\"" :
                              "ps -ef")) {
        if (pattern.matcher(line).find()) {
          seconds++;
          searchForCommand = true;
          break;
        }
      }
    }

    // yes in the mean time someone could use our sleep time
    // but this should be a fairly rare scenerio

    String command = "sleep " + seconds;
    Pattern pattern = Pattern.compile("\b" + command + "\b");

    context.put(ExecSourceConfigurationConstants.CONFIG_RESTART, "false");

    context.put("command", command);
    Configurables.configure(source, context);

    source.start();
    Thread.sleep(1000L);
    source.stop();
    Thread.sleep(1000L);
    for (String line : exec(SystemUtils.IS_OS_WINDOWS ?
                            "cmd /c tasklist /FI \"SESSIONNAME eq Console\"" :
                            "ps -ef")) {
      if(pattern.matcher(line).find()) {
        Assert.fail("Found [" + line + "]");
      }
    }
  }

  private static List<String> exec(String command) throws Exception {
    String[] commandArgs = command.split("\\s+");
    Process process = new ProcessBuilder(commandArgs).start();
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(
          new InputStreamReader(process.getInputStream()));
      List<String> result = Lists.newArrayList();
      String line;
      while((line = reader.readLine()) != null) {
        result.add(line);
      }
      return result;
    } finally {
      process.destroy();
      if(reader != null) {
        reader.close();
      }
      int exit = process.waitFor();
      if(exit != 0) {
        throw new IllegalStateException("Command [" + command + "] exited with "
            + exit);
      }
    }
  }
}
