/*
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
package org.apache.flume.source.jms;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JMSSource extends AbstractPollableSource {
  private static final Logger logger = LoggerFactory.getLogger(JMSSource.class);

  // setup by constructor
  private final JMSMessageConsumerFactory consumerFactory;
  private final InitialContextFactory initialContextFactory;

  // setup by configuration
  private ConnectionFactory connectionFactory;
  private int batchSize;
  private JMSMessageConverter converter;
  private JMSMessageConsumer consumer;
  private String initialContextFactoryName;
  private String providerUrl;
  private String destinationName;
  private JMSDestinationType destinationType;
  private JMSDestinationLocator destinationLocator;
  private String messageSelector;
  private Optional<String> userName;
  private Optional<String> password;
  private SourceCounter sourceCounter;
  private int errorThreshold;
  private long pollTimeout;

  private int jmsExceptionCounter;
  private InitialContext initialContext;


  public JMSSource() {
    this(new JMSMessageConsumerFactory(), new InitialContextFactory());
  }
  @VisibleForTesting
  public JMSSource(JMSMessageConsumerFactory consumerFactory, InitialContextFactory initialContextFactory) {
    super();
    this.consumerFactory = consumerFactory;
    this.initialContextFactory = initialContextFactory;

  }

  @Override
  protected void doConfigure(Context context) throws FlumeException {
    sourceCounter = new SourceCounter(getName());

    initialContextFactoryName = context.getString(JMSSourceConfiguration.
        INITIAL_CONTEXT_FACTORY, "").trim();

    providerUrl = context.getString(JMSSourceConfiguration.PROVIDER_URL, "")
        .trim();

    destinationName = context.getString(JMSSourceConfiguration.
        DESTINATION_NAME, "").trim();

    String destinationTypeName = context.getString(JMSSourceConfiguration.
        DESTINATION_TYPE, "").trim().toUpperCase();

    String destinationLocatorName = context.getString(JMSSourceConfiguration.
        DESTINATION_LOCATOR, JMSSourceConfiguration.DESTINATION_LOCATOR_DEFAULT)
      .trim().toUpperCase();

    messageSelector = context.getString(JMSSourceConfiguration.
        MESSAGE_SELECTOR, "").trim();

    batchSize = context.getInteger(JMSSourceConfiguration.BATCH_SIZE,
        JMSSourceConfiguration.BATCH_SIZE_DEFAULT);

    errorThreshold = context.getInteger(JMSSourceConfiguration.ERROR_THRESHOLD,
        JMSSourceConfiguration.ERROR_THRESHOLD_DEFAULT);

    userName = Optional.fromNullable(context.getString(JMSSourceConfiguration.
        USERNAME));

    pollTimeout = context.getLong(JMSSourceConfiguration.POLL_TIMEOUT,
        JMSSourceConfiguration.POLL_TIMEOUT_DEFAULT);

    String passwordFile = context.getString(JMSSourceConfiguration.
        PASSWORD_FILE, "").trim();

    if(passwordFile.isEmpty()) {
      password = Optional.of("");
    } else {
      try {
        password = Optional.of(Files.toString(new File(passwordFile),
            Charsets.UTF_8).trim());
      } catch (IOException e) {
        throw new FlumeException(String.format(
            "Could not read password file %s", passwordFile), e);
      }
    }

    String converterClassName = context.getString(
        JMSSourceConfiguration.CONVERTER_TYPE,
        JMSSourceConfiguration.CONVERTER_TYPE_DEFAULT)
        .trim();
    if(JMSSourceConfiguration.CONVERTER_TYPE_DEFAULT.
        equalsIgnoreCase(converterClassName)) {
      converterClassName = DefaultJMSMessageConverter.Builder.class.getName();
    }
    Context converterContext = new Context(context.
        getSubProperties(JMSSourceConfiguration.CONVERTER + "."));
    try {
      @SuppressWarnings("rawtypes")
      Class clazz = Class.forName(converterClassName);
      boolean isBuilder = JMSMessageConverter.Builder.class
          .isAssignableFrom(clazz);
      if(isBuilder) {
        JMSMessageConverter.Builder builder = (JMSMessageConverter.Builder)
            clazz.newInstance();
        converter = builder.build(converterContext);
      } else {
        Preconditions.checkState(JMSMessageConverter.class.
            isAssignableFrom(clazz), String.
            format("Class %s is not a subclass of JMSMessageConverter",
                clazz.getName()));
        converter = (JMSMessageConverter)clazz.newInstance();
        boolean configured = Configurables.configure(converter,
            converterContext);
        if(logger.isDebugEnabled()) {
          logger.debug(String.
              format("Attempted configuration of %s, result = %s",
                  converterClassName, String.valueOf(configured)));
        }
      }
    } catch(Exception e) {
      throw new FlumeException(String.format(
          "Unable to create instance of converter %s", converterClassName), e);
    }

    String connectionFactoryName = context.getString(JMSSourceConfiguration.
        CONNECTION_FACTORY, JMSSourceConfiguration.CONNECTION_FACTORY_DEFAULT)
        .trim();

    assertNotEmpty(initialContextFactoryName, String.format(
        "Initial Context Factory is empty. This is specified by %s",
        JMSSourceConfiguration.INITIAL_CONTEXT_FACTORY));

    assertNotEmpty(providerUrl, String.format(
        "Provider URL is empty. This is specified by %s",
        JMSSourceConfiguration.PROVIDER_URL));

    assertNotEmpty(destinationName, String.format(
        "Destination Name is empty. This is specified by %s",
        JMSSourceConfiguration.DESTINATION_NAME));

    assertNotEmpty(destinationTypeName, String.format(
        "Destination Type is empty. This is specified by %s",
        JMSSourceConfiguration.DESTINATION_TYPE));

    try {
      destinationType = JMSDestinationType.valueOf(destinationTypeName);
    } catch (IllegalArgumentException e) {
      throw new FlumeException(String.format("Destination type '%s' is " +
          "invalid.", destinationTypeName), e);
    }

    try {
      destinationLocator = JMSDestinationLocator.valueOf(destinationLocatorName);
    } catch (IllegalArgumentException e) {
      throw new FlumeException(String.format("Destination locator '%s' is " +
          "invalid.", destinationLocatorName), e);
    }

    Preconditions.checkArgument(batchSize > 0, "Batch size must be greater " +
        "than 0");

    try {
      Properties contextProperties = new Properties();
      contextProperties.setProperty(
          javax.naming.Context.INITIAL_CONTEXT_FACTORY,
          initialContextFactoryName);
      contextProperties.setProperty(
          javax.naming.Context.PROVIDER_URL, providerUrl);
      initialContext = initialContextFactory.create(contextProperties);
    } catch (NamingException e) {
      throw new FlumeException(String.format(
          "Could not create initial context %s provider %s",
          initialContextFactoryName, providerUrl), e);
    }

    try {
      connectionFactory = (ConnectionFactory) initialContext.
          lookup(connectionFactoryName);
    } catch (NamingException e) {
      throw new FlumeException("Could not lookup ConnectionFactory", e);
    }
  }

  private void assertNotEmpty(String arg, String msg) {
    Preconditions.checkArgument(!arg.isEmpty(),
        msg);
  }

  @Override
  protected synchronized Status doProcess() throws EventDeliveryException {
    boolean error = true;
    try {
      if(consumer == null) {
        consumer = createConsumer();
      }
      List<Event> events = consumer.take();
      int size = events.size();
      if(size == 0) {
        error = false;
        return Status.BACKOFF;
      }
      sourceCounter.incrementAppendBatchReceivedCount();
      sourceCounter.addToEventReceivedCount(size);
      getChannelProcessor().processEventBatch(events);
      error = false;
      sourceCounter.addToEventAcceptedCount(size);
      sourceCounter.incrementAppendBatchAcceptedCount();
      return Status.READY;
    } catch (ChannelException channelException) {
      logger.warn("Error appending event to channel. "
          + "Channel might be full. Consider increasing the channel "
          + "capacity or make sure the sinks perform faster.", channelException);
    } catch(JMSException jmsException) {
      logger.warn("JMSException consuming events", jmsException);
      if(++jmsExceptionCounter > errorThreshold) {
        if(consumer != null) {
          logger.warn("Exceeded JMSException threshold, closing consumer");
          consumer.rollback();
          consumer.close();
          consumer = null;
        }
      }
    } catch(Throwable throwable) {
      logger.error("Unexpected error processing events", throwable);
      if(throwable instanceof Error) {
        throw (Error) throwable;
      }
    } finally {
      if(error) {
        if(consumer != null) {
          consumer.rollback();
        }
      } else {
        if(consumer != null) {
          consumer.commit();
          jmsExceptionCounter = 0;
        }
      }
    }
    return Status.BACKOFF;
  }
  @Override
  protected synchronized void doStart() {
    try {
      consumer = createConsumer();
      jmsExceptionCounter = 0;
      sourceCounter.start();
    } catch (JMSException e) {
      throw new FlumeException("Unable to create consumer", e);
    }
  }

  @Override
  protected synchronized void doStop() {
    if(consumer != null) {
      consumer.close();
      consumer = null;
    }
    sourceCounter.stop();
  }
  private JMSMessageConsumer createConsumer() throws JMSException {
    logger.info("Creating new consumer for " + destinationName);
    JMSMessageConsumer consumer = consumerFactory.create(initialContext,
    connectionFactory, destinationName, destinationType, destinationLocator,
    messageSelector, batchSize,
        pollTimeout, converter, userName, password);
    jmsExceptionCounter = 0;
    return consumer;
  }
}