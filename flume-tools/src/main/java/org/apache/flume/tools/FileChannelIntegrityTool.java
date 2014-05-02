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
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.file.CorruptEventException;
import org.apache.flume.channel.file.Log;
import org.apache.flume.channel.file.LogFile;
import org.apache.flume.channel.file.LogFileV3;
import org.apache.flume.channel.file.LogRecord;
import org.apache.flume.channel.file.Serialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileChannelIntegrityTool implements FlumeTool {
  public static final Logger LOG = LoggerFactory.getLogger
    (FileChannelIntegrityTool.class);

  private final List<File> dataDirs = new ArrayList<File>();

  @Override
  public void run(String[] args) throws IOException, ParseException {
    boolean shouldContinue = parseCommandLineOpts(args);
    if(!shouldContinue) {
      LOG.error("Could not parse command line options. Exiting ...");
      System.exit(1);
    }
    for(File dataDir : dataDirs) {
      File[] dataFiles = dataDir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if(!name.endsWith(Serialization.METADATA_FILENAME)
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
          LogFile.SequentialReader reader =
            new LogFileV3.SequentialReader(dataFile, null, true);
          LogFile.OperationRecordUpdater updater = new LogFile
            .OperationRecordUpdater(dataFile);
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
              if (record != null) {
                record.getEvent();
              } else {
                fileDone = true;
              }
            } catch (CorruptEventException e) {
              LOG.warn("Corruption found in " + dataFile.toString() + " at "
                + eventPosition);
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
  }

  private boolean parseCommandLineOpts(String[] args) throws ParseException {
    Options options = new Options();
    options
      .addOption("l", "dataDirs", true, "Comma-separated list of data " +
        "directories which the tool must verify. This option is mandatory")
      .addOption("h", "help", false, "Display help");

    CommandLineParser parser = new GnuParser();
    CommandLine commandLine = parser.parse(options, args);
    if(commandLine.hasOption("help")) {
      new HelpFormatter().printHelp("java -jar fcintegritytool ",
        options, true);
      return false;
    }
    if(!commandLine.hasOption("dataDirs")) {
      new HelpFormatter().printHelp("java -jar fcintegritytool ", "",
        options, "dataDirs is required.", true);
      return false;
    } else {
      String dataDirStr[] = commandLine.getOptionValue("dataDirs").split(",");
      for(String dataDir : dataDirStr) {
        File f = new File(dataDir);
        if(!f.exists()) {
          throw new FlumeException("Data directory, " + dataDir + " does not " +
            "exist.");
        }
        dataDirs.add(f);
      }
    }
    return true;
  }
}
