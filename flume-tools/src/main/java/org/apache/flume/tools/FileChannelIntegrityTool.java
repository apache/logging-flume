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
package org.apache.flume.tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.file.CorruptEventException;
import org.apache.flume.channel.file.EventUtils;
import org.apache.flume.channel.file.Log;
import org.apache.flume.channel.file.LogFile;
import org.apache.flume.channel.file.LogFileV3;
import org.apache.flume.channel.file.LogRecord;
import org.apache.flume.channel.file.Serialization;
import org.apache.flume.channel.file.TransactionEventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class FileChannelIntegrityTool implements FlumeTool {
  public static final Logger LOG = LoggerFactory.getLogger(FileChannelIntegrityTool.class);

  private final List<File> dataDirs = new ArrayList<File>();

  private EventValidator eventValidator = EventValidator.NOOP_VALIDATOR;

  private long totalPutEvents;
  private long invalidEvents;
  private long eventsWithException;
  private long corruptEvents;
  private long validEvents;
  private long totalChannelEvents;

  @Override
  public void run(String[] args) throws IOException, ParseException {
    boolean shouldContinue = parseCommandLineOpts(args);
    if (!shouldContinue) {
      LOG.error("Could not parse command line options. Exiting ...");
      System.exit(1);
    }
    for (File dataDir : dataDirs) {
      File[] dataFiles = dataDir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (!name.endsWith(Serialization.METADATA_FILENAME)
              && !name.endsWith(Serialization.METADATA_TMP_FILENAME)
              && !name.endsWith(Serialization.OLD_METADATA_FILENAME)
              && !name.equals(Log.FILE_LOCK)) {
            return true;
          }
          return false;
        }
      });
      if (dataFiles != null && dataFiles.length > 0) {
        for (File dataFile : dataFiles) {
          LOG.info("Checking for corruption in " + dataFile.toString());
          LogFile.SequentialReader reader = new LogFileV3.SequentialReader(dataFile, null, true);
          LogFile.OperationRecordUpdater updater = new LogFile.OperationRecordUpdater(dataFile);
          boolean fileDone = false;
          boolean fileBackedup = false;
          while (!fileDone) {
            long eventPosition = 0;
            try {
              // This depends on the fact that events are of the form:
              // Type, length, data.
              eventPosition = reader.getPosition();
              // Try to get the record, if the checksums don't match,
              // this will throw a CorruptEventException - so the real logic
              // is in the catch block below.
              LogRecord record = reader.next();
              totalChannelEvents++;
              if (record != null) {
                TransactionEventRecord recordEvent = record.getEvent();
                Event event = EventUtils.getEventFromTransactionEvent(recordEvent);
                if (event != null) {
                  totalPutEvents++;
                  try {
                    if (!eventValidator.validateEvent(event)) {
                      if (!fileBackedup) {
                        Serialization.copyFile(dataFile, new File(dataFile.getParent(),
                                dataFile.getName() + ".bak"));
                        fileBackedup = true;
                      }
                      invalidEvents++;
                      updater.markRecordAsNoop(eventPosition);
                    } else {
                      validEvents++;
                    }
                  } catch (Exception e) {
                    // OOPS, didn't expected an exception
                    // considering as failure case
                    // marking as noop
                    System.err.println("Encountered Exception while validating event, " +
                                       "marking as invalid");
                    updater.markRecordAsNoop(eventPosition);
                    eventsWithException++;
                  }
                }
              } else {
                fileDone = true;
              }
            } catch (CorruptEventException e) {
              corruptEvents++;
              totalChannelEvents++;
              LOG.warn("Corruption found in " + dataFile.toString() + " at " + eventPosition);
              if (!fileBackedup) {
                Serialization.copyFile(dataFile, new File(dataFile.getParent(),
                                                          dataFile.getName() + ".bak"));
                fileBackedup = true;
              }
              updater.markRecordAsNoop(eventPosition);
            }
          }
          updater.close();
          reader.close();
        }
      }
    }
    printSummary();
  }

  private boolean parseCommandLineOpts(String[] args) throws ParseException {
    Options options = new Options();
    options.addOption("l", "dataDirs", true, "Comma-separated list of data " +
                      "directories which the tool must verify. This option is mandatory")
           .addOption("h", "help", false, "Display help")
           .addOption("e", "eventValidator", true,
                      "Fully Qualified Name of Event Validator Implementation");

    Option property = OptionBuilder.withArgName("property=value")
            .hasArgs(2)
            .withValueSeparator()
            .withDescription("custom properties")
            .create("D");

    options.addOption(property);

    CommandLineParser parser = new GnuParser();
    CommandLine commandLine = parser.parse(options, args);
    if (commandLine.hasOption("help")) {
      new HelpFormatter().printHelp("bin/flume-ng tool fcintegritytool ", options, true);
      return false;
    }
    if (!commandLine.hasOption("dataDirs")) {
      new HelpFormatter().printHelp("bin/flume-ng tool fcintegritytool ", "",
          options, "dataDirs is required.", true);
      return false;
    } else {
      String[] dataDirStr = commandLine.getOptionValue("dataDirs").split(",");
      for (String dataDir : dataDirStr) {
        File f = new File(dataDir);
        if (!f.exists()) {
          throw new FlumeException("Data directory, " + dataDir + " does not exist.");
        }
        dataDirs.add(f);
      }
    }

    if (commandLine.hasOption("eventValidator")) {
      try {
        Class<? extends EventValidator.Builder> eventValidatorClassName =
            (Class<? extends EventValidator.Builder>)Class.forName(
                commandLine.getOptionValue("eventValidator"));
        EventValidator.Builder eventValidatorBuilder = eventValidatorClassName.newInstance();

        // Pass on the configuration parameter
        Properties systemProperties = commandLine.getOptionProperties("D");
        Context context = new Context();

        Set<String> keys = systemProperties.stringPropertyNames();
        for (String key : keys) {
          context.put(key, systemProperties.getProperty(key));
        }
        eventValidatorBuilder.configure(context);
        eventValidator = eventValidatorBuilder.build();
      } catch (Exception e) {
        System.err.println(String.format("Could find class %s in lib folder",
                commandLine.getOptionValue("eventValidator")));
        e.printStackTrace();
        return false;
      }
    }
    return true;
  }

  /**
   * Prints the summary of run. Following information is printed
   *
   */
  private void printSummary() {
    System.out.println("---------- Summary --------------------");
    System.out.println("Number of Events in the Channel = " + totalChannelEvents++);
    System.out.println("Number of Put Events Processed = " + totalPutEvents);
    System.out.println("Number of Valid Put Events = " + validEvents);
    System.out.println("Number of Invalid Put Events = " + invalidEvents);
    System.out.println("Number of Put Events that threw Exception during validation = "
        + eventsWithException);
    System.out.println("Number of Corrupt Events = " + corruptEvents);
    System.out.println("---------------------------------------");
  }
}
