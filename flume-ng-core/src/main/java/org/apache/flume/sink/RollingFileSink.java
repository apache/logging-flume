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
package org.apache.flume.sink;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.PathManager;
import org.apache.flume.instrumentation.SinkCounter;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;

public class RollingFileSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(RollingFileSink.class);
  private static final long defaultRollInterval = 30;
  private static final int defaultBatchSize = 100;

  private int batchSize = defaultBatchSize;
  
  private long maxFileSize = -1;
  
  /** how many old files to keep **/ 
  private int maxHistory = -1;

  private File directory;
  private long rollInterval;
  private OutputStream outputStream;
  private long bytesWrittenToStream = 0;
  
  private ScheduledExecutorService rollService;

  private String serializerType;
  private Context serializerContext;
  private EventSerializer serializer;

  private SinkCounter sinkCounter;

  private PathManager pathController;
  private volatile boolean shouldRotate;

  public RollingFileSink() {
    shouldRotate = false;
  }

  @Override
  public void configure(Context context) {
    String directory = context.getString("sink.directory");
    String rollInterval = context.getString("sink.rollInterval");
    
    String fileName = context.getString("sink.fileName");
    
    
    serializerType = context.getString("sink.serializer", "TEXT");
    serializerContext =
        new Context(context.getSubProperties("sink." +
            EventSerializer.CTX_PREFIX));

    Preconditions.checkArgument(directory != null, "Directory may not be null");
    Preconditions.checkNotNull(serializerType, "Serializer type is undefined");
    Preconditions.checkArgument(StringUtils.isNotBlank(fileName), "File name may not be null");
    
    this.directory = new File(directory);
    
    pathController = new PathManager(fileName, getFiles(this.directory, fileName).length);

    if (rollInterval == null) {
      this.rollInterval = defaultRollInterval;
    } else {
      this.rollInterval = Long.parseLong(rollInterval);
    }

    batchSize = context.getInteger("sink.batchSize", defaultBatchSize);

    

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
    
    if(StringUtils.isNotBlank(context.getString("sink.maxFileSize"))) {
      maxFileSize = parseFileSize(context.getString("sink.maxFileSize"));
    }
    
    if(StringUtils.isNotBlank(context.getString("sink.maxHistory"))) {
      maxHistory = Integer.parseInt(context.getString("sink.maxHistory"));
    }
  }
  
  private long parseFileSize(String fileSizeStr) {
    int multiplier = 1;
    long size = 0;
    fileSizeStr = fileSizeStr.toUpperCase();
    if(fileSizeStr.endsWith("GB")) {
      multiplier = (int)Math.pow(1024, 3);
      size = Long.valueOf(fileSizeStr.substring(0, fileSizeStr.length() - 2));
    } else if(fileSizeStr.endsWith("MB")) {
      multiplier = (int)Math.pow(1024, 2);
      size = Long.valueOf(fileSizeStr.substring(0, fileSizeStr.length() - 2));
    } else if(fileSizeStr.endsWith("KB")) {
      multiplier = 1024;
      size = Long.valueOf(fileSizeStr.substring(0, fileSizeStr.length() - 2));
    }  else if(fileSizeStr.endsWith("B")) {
      size = Long.valueOf(fileSizeStr.substring(0, fileSizeStr.length() - 1));
    }  else {
      size = Long.valueOf(fileSizeStr);
    }
    
    return multiplier  * size;
  }
  

  @Override
  public void start() {
    logger.info("Starting {}...", this);
    sinkCounter.start();
    super.start();

    pathController.setBaseDirectory(directory);
    if(rollInterval > 0){
      setResetRollService();
    } else{
      logger.info("RollInterval is not valid, file rolling will not happen.");
    }
    logger.info("RollingFileSink {} started.", getName());
  }

  @Override
  public Status process() throws EventDeliveryException {
    
    if (shouldRotate) {
      logger.debug("Time to rotate {}", pathController.getCurrentFile());
      rotateFile();
    }
    
    if(outputStream == null) {
      initSerializer();
    }

    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Event event = null;
    Status result = Status.READY;

    try {
      transaction.begin();
      int eventAttemptCounter = 0;
      for (int i = 0; i < batchSize; i++) {
        event = channel.take();
        if (event != null) {
          if(!canWriteEvent(event)) {
            // rotate the file and reset timer for time based rolling
            Log.info("Hit max file size limit, rolling file and resetting roll service.");
            rotateFile();
            setResetRollService();
            bytesWrittenToStream = 0;
          } 

          sinkCounter.incrementEventDrainAttemptCount();
          eventAttemptCounter++;
          serializer.write(event);
          /*
           * FIXME: Feature: Control flush interval based on time or number of
           * events. For now, we're super-conservative and flush on each write.
           */
        } else {
          // No events found, request back-off semantics from runner
          result = Status.BACKOFF;
          break;
        }
      }
      serializer.flush();
      outputStream.flush();
      transaction.commit();
      sinkCounter.addToEventDrainSuccessCount(eventAttemptCounter);
    } catch (Exception ex) {
      transaction.rollback();
      throw new EventDeliveryException("Failed to process transaction", ex);
    } finally {
      transaction.close();
    }

    return result;
  }

  /**
   * Method to start/reset service to roll log file. This should be called only from {{@link #start()} 
   * or from {@link #process()} when we rotate file after it hits max size
   * 
   */
  private void setResetRollService() {
    
    if(rollService != null) {
      rollService.shutdownNow();
    }
    
    rollService = Executors.newScheduledThreadPool(
        1,
        new ThreadFactoryBuilder().setNameFormat(
            "rollingFileSink-roller-" +
        Thread.currentThread().getId() + "-%d").build());

    /*
     * Every N seconds, mark that it's time to rotate. We purposefully do NOT
     * touch anything other than the indicator flag to avoid error handling
     * issues (e.g. IO exceptions occuring in two different threads.
     * Resist the urge to actually perform rotation in a separate thread!
     */
    rollService.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        logger.debug("Marking time to rotate file {}",
            pathController.getCurrentFile());
        shouldRotate = true;
      }

    }, rollInterval, rollInterval, TimeUnit.SECONDS);
  }
  

  private boolean canWriteEvent(Event event) {
    if(maxFileSize <= 0) {
      return true;
    }
    
    // We are going conservative as we don't know if underlying serializer writes just the body or both body and header 
    long bytesToWrite = (event.getHeaders() + " ").getBytes().length + event.getBody().length;
    return (bytesWrittenToStream + bytesToWrite) < maxFileSize;
  }
  
  
  private void initSerializer() throws EventDeliveryException {
    // Open a new output stream
    if (outputStream == null) {
      File currentFile = pathController.getCurrentFile();
      logger.debug("Opening output stream for file {}", currentFile);
      try {
        outputStream = new BufferedOutputStream(
            new FileOutputStream(currentFile));
        serializer = EventSerializerFactory.getInstance(
            serializerType, serializerContext, outputStream);
        serializer.afterCreate();
        sinkCounter.incrementConnectionCreatedCount();
      } catch (IOException e) {
        sinkCounter.incrementConnectionFailedCount();
        throw new EventDeliveryException("Failed to open file "
            + pathController.getCurrentFile() + " while delivering event", e);
      }
    }
  }

  private void rotateFile() throws EventDeliveryException {
    if (outputStream != null) {
      logger.debug("Closing file {}", pathController.getCurrentFile());

      try {
        serializer.flush();
        serializer.beforeClose();
        outputStream.close();
        sinkCounter.incrementConnectionClosedCount();
        shouldRotate = false;
      } catch (IOException e) {
        sinkCounter.incrementConnectionFailedCount();
        throw new EventDeliveryException("Unable to rotate file "
            + pathController.getCurrentFile() + " while delivering event", e);
      } finally {
        serializer = null;
        outputStream = null;
      }
      
      pathController.rotate();
    }
    
    initSerializer();
    
    // delete old files in a separate thread
    if(maxHistory > 0) {
     new Thread( new Runnable() {
        @Override
        public void run() {
          deleteOldFiles();
        }
      }).start();
      
    }
  }

  private void deleteOldFiles() {
    File [] files = getFiles(pathController.getBaseDirectory(), pathController.getFileName());

    if(files.length > maxHistory) {
      logger.info("Reached to {} files. Deleting old files as per maxHistory rule", files.length);
      
      // sort files by last modified time (damn I can't use Streams)

      //Comparator<File> comparator = Comparator.comparing(File::lastModified);
      //Collections.sort(files, comparator.reversed());

      Arrays.sort(files, new Comparator<File>() {
        @Override
        public int compare(File f1, File f2) {
          return (int) (f1.lastModified() - f2.lastModified());
        }
      });

      for(int i = 0; i < (files.length - maxHistory); i++ ) {
        files[i].delete();
      }
    }
  }
  
  private File[] getFiles(File directory, final String fileName) {
    File logDiretory = directory;
    
    if(!logDiretory.exists()) {
      return new File[0];
    }
    
    File [] files = logDiretory.listFiles(new FilenameFilter() {
      
      private Pattern p = Pattern.compile(fileName+".*");
      
       @Override
       public boolean accept(File dir, String name) {
         return p.matcher(name).matches() && !name.equals(fileName);
       }
     });
    
    return files;
  }

  @Override
  public void stop() {
    logger.info("RollingFile sink {} stopping...", getName());
    sinkCounter.stop();
    super.stop();

    if (outputStream != null) {
      logger.debug("Closing file {}", pathController.getCurrentFile());

      try {
        serializer.flush();
        serializer.beforeClose();
        outputStream.close();
        sinkCounter.incrementConnectionClosedCount();
      } catch (IOException e) {
        sinkCounter.incrementConnectionFailedCount();
        logger.error("Unable to close output stream. Exception follows.", e);
      } finally {
        outputStream = null;
        serializer = null;
      }
    }
    if(rollInterval > 0){
      rollService.shutdown();

      while (!rollService.isTerminated()) {
        try {
          rollService.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          logger
          .debug(
              "Interrupted while waiting for roll service to stop. " +
              "Please report this.", e);
        }
      }
    }
    logger.info("RollingFile sink {} stopped. Event metrics: {}",
        getName(), sinkCounter);
  }

  public File getDirectory() {
    return directory;
  }

  public void setDirectory(File directory) {
    this.directory = directory;
  }

  public long getRollInterval() {
    return rollInterval;
  }

  public void setRollInterval(long rollInterval) {
    this.rollInterval = rollInterval;
  }

}
