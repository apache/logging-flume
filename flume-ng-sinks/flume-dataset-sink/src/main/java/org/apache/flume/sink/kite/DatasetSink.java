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
package org.apache.flume.sink.kite;

import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.sink.kite.parser.EntityParserFactory;
import org.apache.flume.sink.kite.parser.EntityParser;
import org.apache.flume.sink.kite.policy.FailurePolicy;
import org.apache.flume.sink.kite.policy.FailurePolicyFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import java.net.URI;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Flushable;
import org.kitesdk.data.Syncable;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flume.sink.kite.DatasetSinkConstants.*;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;

/**
 * Sink that writes events to a Kite Dataset. This sink will parse the body of
 * each incoming event and store the resulting entity in a Kite Dataset. It
 * determines the destination Dataset by opening a dataset URI
 * {@code kite.dataset.uri} or opening a repository URI, {@code kite.repo.uri},
 * and loading a Dataset by name, {@code kite.dataset.name}, and namespace,
 * {@code kite.dataset.namespace}.
 */
public class DatasetSink extends AbstractSink implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetSink.class);

  private Context context = null;
  private PrivilegedExecutor privilegedExecutor;

  private String datasetName = null;
  private URI datasetUri = null;
  private Schema datasetSchema = null;
  private DatasetWriter<GenericRecord> writer = null;

  /**
   * The number of events to process as a single batch.
   */
  private long batchSize = DEFAULT_BATCH_SIZE;

  /**
   * The number of seconds to wait before rolling a writer.
   */
  private int rollIntervalSeconds = DEFAULT_ROLL_INTERVAL;

  /**
   * Flag that says if Flume should commit on every batch.
   */
  private boolean commitOnBatch = DEFAULT_FLUSHABLE_COMMIT_ON_BATCH;

  /**
   * Flag that says if Flume should sync on every batch.
   */
  private boolean syncOnBatch = DEFAULT_SYNCABLE_SYNC_ON_BATCH;

  /**
   * The last time the writer rolled.
   */
  private long lastRolledMillis = 0L;

  /**
   * The raw number of bytes parsed.
   */
  private long bytesParsed = 0L;

  /**
   * A class for parsing Kite entities from Flume Events.
   */
  private EntityParser<GenericRecord> parser = null;

  /**
   * A class implementing a failure newPolicy for events that had a
 non-recoverable error during processing.
   */
  private FailurePolicy failurePolicy = null;

  private SinkCounter counter = null;

  /**
   * The Kite entity
   */
  private GenericRecord entity = null;
  // TODO: remove this after PARQUET-62 is released
  private boolean reuseEntity = true;

  /**
   * The Flume transaction. Used to keep transactions open across calls to
   * process.
   */
  private Transaction transaction = null;

  /**
   * Internal flag on if there has been a batch of records committed. This is
   * used during rollback to know if the current writer needs to be closed.
   */
  private boolean committedBatch = false;

  // Factories
  private static final EntityParserFactory ENTITY_PARSER_FACTORY =
      new EntityParserFactory();
  private static final FailurePolicyFactory FAILURE_POLICY_FACTORY =
      new FailurePolicyFactory();

  /**
   * Return the list of allowed formats.
   * @return The list of allowed formats.
   */
  protected List<String> allowedFormats() {
    return Lists.newArrayList("avro", "parquet");
  }

  @Override
  public void configure(Context context) {
    this.context = context;

    String principal = context.getString(AUTH_PRINCIPAL);
    String keytab = context.getString(AUTH_KEYTAB);
    String effectiveUser = context.getString(AUTH_PROXY_USER);

    this.privilegedExecutor = FlumeAuthenticationUtil.getAuthenticator(
            principal, keytab).proxyAs(effectiveUser);

    // Get the dataset URI and name from the context
    String datasetURI = context.getString(CONFIG_KITE_DATASET_URI);
    if (datasetURI != null) {
      this.datasetUri = URI.create(datasetURI);
      this.datasetName = uriToName(datasetUri);
    } else {
      String repositoryURI = context.getString(CONFIG_KITE_REPO_URI);
      Preconditions.checkNotNull(repositoryURI, "No dataset configured. Setting "
          + CONFIG_KITE_DATASET_URI + " is required.");

      this.datasetName = context.getString(CONFIG_KITE_DATASET_NAME);
      Preconditions.checkNotNull(datasetName, "No dataset configured. Setting "
          + CONFIG_KITE_DATASET_URI + " is required.");

      String namespace = context.getString(CONFIG_KITE_DATASET_NAMESPACE,
          DEFAULT_NAMESPACE);

      this.datasetUri = new URIBuilder(repositoryURI, namespace, datasetName)
          .build();
    }
    this.setName(datasetUri.toString());

    if (context.getBoolean(CONFIG_SYNCABLE_SYNC_ON_BATCH,
        DEFAULT_SYNCABLE_SYNC_ON_BATCH)) {
      Preconditions.checkArgument(
          context.getBoolean(CONFIG_FLUSHABLE_COMMIT_ON_BATCH,
              DEFAULT_FLUSHABLE_COMMIT_ON_BATCH), "Configuration error: "
                  + CONFIG_FLUSHABLE_COMMIT_ON_BATCH + " must be set to true when "
                  + CONFIG_SYNCABLE_SYNC_ON_BATCH + " is set to true.");
    }

    // Create the configured failure failurePolicy
    this.failurePolicy = FAILURE_POLICY_FACTORY.newPolicy(context);

    // other configuration
    this.batchSize = context.getLong(CONFIG_KITE_BATCH_SIZE,
        DEFAULT_BATCH_SIZE);
    this.rollIntervalSeconds = context.getInteger(CONFIG_KITE_ROLL_INTERVAL,
        DEFAULT_ROLL_INTERVAL);

    this.counter = new SinkCounter(datasetName);
  }

  @Override
  public synchronized void start() {
    this.lastRolledMillis = System.currentTimeMillis();
    counter.start();
    // signal that this sink is ready to process
    LOG.info("Started DatasetSink " + getName());
    super.start();
  }

  /**
   * Causes the sink to roll at the next {@link #process()} call.
   */
  @VisibleForTesting
  void roll() {
    this.lastRolledMillis = 0L;
  }

  @VisibleForTesting
  DatasetWriter<GenericRecord> getWriter() {
    return writer;
  }

  @VisibleForTesting
  void setWriter(DatasetWriter<GenericRecord> writer) {
    this.writer = writer;
  }

  @VisibleForTesting
  void setParser(EntityParser<GenericRecord> parser) {
    this.parser = parser;
  }

  @VisibleForTesting
  void setFailurePolicy(FailurePolicy failurePolicy) {
    this.failurePolicy = failurePolicy;
  }

  @Override
  public synchronized void stop() {
    counter.stop();

    try {
      // Close the writer and commit the transaction, but don't create a new
      // writer since we're stopping
      closeWriter();
      commitTransaction();
    } catch (EventDeliveryException ex) {
      rollbackTransaction();

      LOG.warn("Closing the writer failed: " + ex.getLocalizedMessage());
      LOG.debug("Exception follows.", ex);
      // We don't propogate the exception as the transaction would have been
      // rolled back and we can still finish stopping
    }

  // signal that this sink has stopped
    LOG.info("Stopped dataset sink: " + getName());
    super.stop();
  }

  @Override
  public Status process() throws EventDeliveryException {
    long processedEvents = 0;

    try {
      if (shouldRoll()) {
        closeWriter();
        commitTransaction();
        createWriter();
      }

      // The writer shouldn't be null at this point
      Preconditions.checkNotNull(writer,
          "Can't process events with a null writer. This is likely a bug.");
      Channel channel = getChannel();

      // Enter the transaction boundary if we haven't already
      enterTransaction(channel);

      for (; processedEvents < batchSize; processedEvents += 1) {
        Event event = channel.take();

        if (event == null) {
          // no events available in the channel
          break;
        }

        write(event);
      }

      // commit transaction
      if (commitOnBatch) {
        // Flush/sync before commiting. A failure here will result in rolling back
        // the transaction
        if (syncOnBatch && writer instanceof Syncable) {
          ((Syncable) writer).sync();
        } else if (writer instanceof Flushable) {
          ((Flushable) writer).flush();
        }
        boolean committed = commitTransaction();
        Preconditions.checkState(committed,
            "Tried to commit a batch when there was no transaction");
        committedBatch |= committed;
      }
    } catch (Throwable th) {
      // catch-all for any unhandled Throwable so that the transaction is
      // correctly rolled back.
      rollbackTransaction();

      if (commitOnBatch && committedBatch) {
        try {
          closeWriter();
        } catch (EventDeliveryException ex) {
          LOG.warn("Error closing writer there may be temp files that need to"
              + " be manually recovered: " + ex.getLocalizedMessage());
          LOG.debug("Exception follows.", ex);
        }
      } else {
        this.writer = null;
      }

      // handle the exception
      Throwables.propagateIfInstanceOf(th, Error.class);
      Throwables.propagateIfInstanceOf(th, EventDeliveryException.class);
      throw new EventDeliveryException(th);
    }

    if (processedEvents == 0) {
      counter.incrementBatchEmptyCount();
      return Status.BACKOFF;
    } else if (processedEvents < batchSize) {
      counter.incrementBatchUnderflowCount();
    } else {
      counter.incrementBatchCompleteCount();
    }

    counter.addToEventDrainSuccessCount(processedEvents);

    return Status.READY;
  }

  /**
   * Parse the event using the entity parser and write the entity to the dataset.
   *
   * @param event The event to write
   * @throws EventDeliveryException An error occurred trying to write to the
                                dataset that couldn't or shouldn't be
                                handled by the failure policy.
   */
  @VisibleForTesting
  void write(Event event) throws EventDeliveryException {
    try {
      this.entity = parser.parse(event, reuseEntity ? entity : null);
      this.bytesParsed += event.getBody().length;

      // writeEncoded would be an optimization in some cases, but HBase
      // will not support it and partitioned Datasets need to get partition
      // info from the entity Object. We may be able to avoid the
      // serialization round-trip otherwise.
      writer.write(entity);
    } catch (NonRecoverableEventException ex) {
      failurePolicy.handle(event, ex);
    } catch (DataFileWriter.AppendWriteException ex) {
      failurePolicy.handle(event, ex);
    } catch (RuntimeException ex) {
      Throwables.propagateIfInstanceOf(ex, EventDeliveryException.class);
      throw new EventDeliveryException(ex);
    }
  }

  /**
   * Create a new writer.
   *
   * This method also re-loads the dataset so updates to the configuration or
   * a dataset created after Flume starts will be loaded.
   *
   * @throws EventDeliveryException There was an error creating the writer.
   */
  @VisibleForTesting
  void createWriter() throws EventDeliveryException {
    // reset the commited flag whenever a new writer is created
    committedBatch = false;
    try {
      View<GenericRecord> view;

      view = privilegedExecutor.execute(
        new PrivilegedAction<Dataset<GenericRecord>>() {
          @Override
          public Dataset<GenericRecord> run() {
            return Datasets.load(datasetUri);
          }
        });

      DatasetDescriptor descriptor = view.getDataset().getDescriptor();
      Format format = descriptor.getFormat();
      Preconditions.checkArgument(allowedFormats().contains(format.getName()),
          "Unsupported format: " + format.getName());

      Schema newSchema = descriptor.getSchema();
      if (datasetSchema == null || !newSchema.equals(datasetSchema)) {
        this.datasetSchema = descriptor.getSchema();
        // dataset schema has changed, create a new parser
        parser = ENTITY_PARSER_FACTORY.newParser(datasetSchema, context);
      }

      this.reuseEntity = !(Formats.PARQUET.equals(format));

      // TODO: Check that the format implements Flushable after CDK-863
      // goes in. For now, just check that the Dataset is Avro format
      this.commitOnBatch = context.getBoolean(CONFIG_FLUSHABLE_COMMIT_ON_BATCH,
          DEFAULT_FLUSHABLE_COMMIT_ON_BATCH) && (Formats.AVRO.equals(format));

      // TODO: Check that the format implements Syncable after CDK-863
      // goes in. For now, just check that the Dataset is Avro format
      this.syncOnBatch = context.getBoolean(CONFIG_SYNCABLE_SYNC_ON_BATCH,
          DEFAULT_SYNCABLE_SYNC_ON_BATCH) && (Formats.AVRO.equals(format));

      this.datasetName = view.getDataset().getName();

      this.writer = view.newWriter();

      // Reset the last rolled time and the metrics
      this.lastRolledMillis = System.currentTimeMillis();
      this.bytesParsed = 0L;
    } catch (DatasetNotFoundException ex) {
      throw new EventDeliveryException("Dataset " + datasetUri + " not found."
          + " The dataset must be created before Flume can write to it.", ex);
    } catch (RuntimeException ex) {
      throw new EventDeliveryException("Error trying to open a new"
          + " writer for dataset " + datasetUri, ex);
    }
  }

  /**
   * Return true if the sink should roll the writer.
   *
   * Currently, this is based on time since the last roll or if the current
   * writer is null.
   *
   * @return True if and only if the sink should roll the writer
   */
  private boolean shouldRoll() {
    long currentTimeMillis = System.currentTimeMillis();
    long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(
        currentTimeMillis - lastRolledMillis);

    LOG.debug("Current time: {}, lastRolled: {}, diff: {} sec",
        new Object[] {currentTimeMillis, lastRolledMillis, elapsedTimeSeconds});

    return elapsedTimeSeconds >= rollIntervalSeconds || writer == null;
  }

  /**
   * Close the current writer.
   *
   * This method always sets the current writer to null even if close fails.
   * If this method throws an Exception, callers *must* rollback any active
   * transaction to ensure that data is replayed.
   *
   * @throws EventDeliveryException
   */
  @VisibleForTesting
  void closeWriter() throws EventDeliveryException {
    if (writer != null) {
      try {
        writer.close();

        long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(
            System.currentTimeMillis() - lastRolledMillis);
        LOG.info("Closed writer for {} after {} seconds and {} bytes parsed",
            new Object[]{datasetUri, elapsedTimeSeconds, bytesParsed});
      } catch (DatasetIOException ex) {
        throw new EventDeliveryException("Check HDFS permissions/health. IO"
            + " error trying to close the  writer for dataset " + datasetUri,
            ex);
      } catch (RuntimeException ex) {
        throw new EventDeliveryException("Error trying to close the  writer for"
            + " dataset " + datasetUri, ex);
      } finally {
        // If we failed to close the writer then we give up on it as we'll
        // end up throwing an EventDeliveryException which will result in
        // a transaction rollback and a replay of any events written during
        // the current transaction. If commitOnBatch is true, you can still
        // end up with orphaned temp files that have data to be recovered.
        this.writer = null;
        failurePolicy.close();
      }
    }
  }

  /**
   * Enter the transaction boundary. This will either begin a new transaction
   * if one didn't already exist. If we're already in a transaction boundary,
   * then this method does nothing.
   *
   * @param channel The Sink's channel
   * @throws EventDeliveryException There was an error starting a new batch
   *                                with the failure policy.
   */
  private void enterTransaction(Channel channel) throws EventDeliveryException {
    // There's no synchronization around the transaction instance because the
    // Sink API states "the Sink#process() call is guaranteed to only
    // be accessed  by a single thread". Technically other methods could be
    // called concurrently, but the implementation of SinkRunner waits
    // for the Thread running process() to end before calling stop()
    if (transaction == null) {
      this.transaction = channel.getTransaction();
      transaction.begin();
      failurePolicy = FAILURE_POLICY_FACTORY.newPolicy(context);
    }
  }

  /**
   * Commit and close the transaction.
   *
   * If this method throws an Exception the caller *must* ensure that the
   * transaction is rolled back. Callers can roll back the transaction by
   * calling {@link #rollbackTransaction()}.
   *
   * @return True if there was an open transaction and it was committed, false
   *         otherwise.
   * @throws EventDeliveryException There was an error ending the batch with
   *                                the failure policy.
   */
  @VisibleForTesting
  boolean commitTransaction() throws EventDeliveryException {
    if (transaction != null) {
      failurePolicy.sync();
      transaction.commit();
      transaction.close();
      this.transaction = null;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Rollback the transaction. If there is a RuntimeException during rollback,
   * it will be logged but the transaction instance variable will still be
   * nullified.
   */
  private void rollbackTransaction() {
    if (transaction != null) {
      try {
        // If the transaction wasn't committed before we got the exception, we
        // need to rollback.
        transaction.rollback();
      } catch (RuntimeException ex) {
        LOG.error("Transaction rollback failed: " + ex.getLocalizedMessage());
        LOG.debug("Exception follows.", ex);
      } finally {
        transaction.close();
        this.transaction = null;
      }
    }
  }

  /**
   * Get the name of the dataset from the URI
   *
   * @param uri The dataset or view URI
   * @return The dataset name
   */
  private static String uriToName(URI uri) {
    return Registration.lookupDatasetUri(URI.create(
        uri.getRawSchemeSpecificPart())).second().get("dataset");
  }
}
