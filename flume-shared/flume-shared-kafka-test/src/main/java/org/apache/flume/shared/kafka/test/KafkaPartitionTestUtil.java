/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.apache.flume.shared.kafka.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;

public class KafkaPartitionTestUtil {

  public static final String PARTITION_HEADER = "partition-header";

  /**
   * This method checks the retrieved messages (passed as resultsMap) against the expected
   * results (passed as partitionMap). The behaviour of this method is slightly different
   * depending on the scenario:
   *  - STATIC_HEADER_ONLY: Don't check partitionMap, just check that all messages are in the
   *                        passed staticPtn partition.
   *  - NO_PARTITION_HEADERS: Check that messages are evenly distributed between partitions
   *                          (requires numMsgs to be a multiple of the number of partitons)
   *  - else: Check that the contents of each partition list in resultsMap is the same as that
   *          specified in partitionMap.
   *
   *  As this is a testing method, it will issue JUnit AssertionExceptions if the results are not
   *  as expected.
   *
   * @param scenario
   * @param partitionMap
   * @param resultsMap
   * @param staticPtn
   * @param numMsgs
   */
  public static void checkResultsAgainstSkew(PartitionTestScenario scenario,
      Map<Integer,List<Event>> partitionMap, Map<Integer, List<byte[]>> resultsMap,
                                 int staticPtn, int numMsgs) {
    int numPtns = partitionMap.size();

    if (scenario == PartitionTestScenario.NO_PARTITION_HEADERS && numMsgs % numPtns != 0) {
      throw new IllegalArgumentException("This method is not designed to work with scenarios" +
                " where there is expected to be a non-even distribution of messages");
    }

    for (int ptn = 0; ptn < numPtns; ptn++) {
      List<Event> expectedResults = partitionMap.get(ptn);
      List<byte[]> actualResults = resultsMap.get(ptn);
      if (scenario == PartitionTestScenario.PARTITION_ID_HEADER_ONLY ||
          scenario == PartitionTestScenario.STATIC_HEADER_AND_PARTITION_ID) {
        // In these two scenarios we're checking against partitionMap
        Assert.assertEquals(expectedResults.size(), actualResults.size());
        //Go and check the message payload is what we wanted it to be
        for (int idx = 0; idx < expectedResults.size(); idx++) {
          Assert.assertArrayEquals(expectedResults.get(idx).getBody(), actualResults.get(idx));
        }
      } else if (scenario == PartitionTestScenario.STATIC_HEADER_ONLY) {
        // Check that if we are looking in the statically assigned partition
        // all messages are in it, else all other partitions are zero
        if (ptn == staticPtn) {
          Assert.assertEquals(numMsgs, actualResults.size());
        } else {
          Assert.assertEquals(0, actualResults.size());
        }
      } else if (scenario == PartitionTestScenario.NO_PARTITION_HEADERS) {
        // Checking for an even distribution
        Assert.assertEquals(numMsgs / numPtns, actualResults.size());
      }
    }
  }

  /**
   * This method is can be used to create a list of events for use in Kafka partition tests.
   * Depending on the scenario it will deliberate generate an artificially skewed distribution
   * of events-per-partition (populated into the passed partitionMap) and then ordered randomly
   * into the resulting List of events.
   * Four scenarios are catered for:
   *  - STATIC_HEADER_ONLY: All events are put into the partition specified by the staticPtn param
   *  - PARTITION_ID_HEADER_ONLY: Events are skewed into three partitions
   *  - STATIC_HEADER_AND_PARTITION_ID: Events are skewed into two partitions, with all others
   *                                    going into the partition specified by staticPtn
   *  - NO_PARTITION_HEADERS: No partition header is set and the partitionMap is not populated
   *
   * @param scenario The scenario being catered for.
   * @param numMsgs The number of messages to generate
   * @param partitionMap A map of Integer (partitionId) and List of Events - to be populated
   * @param numPtns The number of partitions to be populated.
   * @param staticPtn The static partition to be assigned to.
   * @return
   */
  public static List<Event> generateSkewedMessageList(PartitionTestScenario scenario, int numMsgs,
      Map<Integer, List<Event>> partitionMap, int numPtns, int staticPtn) {
    List<Event> msgs = new ArrayList<Event>(numMsgs);

    // Pre-conditions
    if (numMsgs < 0) {
      throw new IllegalArgumentException("Number of messages must be greater than zero");
    }
    if (staticPtn >= numPtns) {
      throw new IllegalArgumentException("The static partition must be less than the " +
                                         "number of partitions");
    }
    if (numPtns < 5) {
      throw new IllegalArgumentException("This method is designed to work with at least 5 " +
                                         "partitions");
    }
    if (partitionMap.size() != numPtns) {
      throw new IllegalArgumentException("partitionMap has not been correctly initialised");
    }

    for (int i = 0; i < numMsgs; i++) {
      Map<String, String> headers = new HashMap<String, String>();
      Integer partition = null;
      // Simple code to artificially create a skew. In this scenario, with 5 partitions
      // and 50 messages we end up with a ratio of 0:0:27:13:10 however these will be
      // interleaved

      if (scenario == PartitionTestScenario.NO_PARTITION_HEADERS) {
        // Don't bother adding a header or computing a partition
      } else if (scenario == PartitionTestScenario.STATIC_HEADER_ONLY) {
        partition = staticPtn;
      } else {
        // We're going to artificially create a skew by putting every 5th event
        // into partition 4, every 3rd event into partition 3 and everything else into
        // partition 2 (unless a static partition is provided, in which case we'll
        // put it into that partition instead, but without setting the header).
        if (i % 5 == 0) {
          partition = 4;
          headers.put(PARTITION_HEADER, String.valueOf(partition));
        } else if (i % 3 == 0 ) {
          partition = 3;
          headers.put(PARTITION_HEADER, String.valueOf(partition));
        } else if (scenario == PartitionTestScenario.STATIC_HEADER_AND_PARTITION_ID) {
          // In this case we're not going to set the header, but we are going
          // to set partition which will then populate the partitionMap
          partition = staticPtn;
        } else if (scenario == PartitionTestScenario.PARTITION_ID_HEADER_ONLY) {
          partition = 2;
          headers.put(PARTITION_HEADER, String.valueOf(partition));
        } // Logically no other scenarios
      }

      // Build the event
      Event event = EventBuilder.withBody(String.valueOf(i).getBytes(), headers);

      if (scenario != PartitionTestScenario.NO_PARTITION_HEADERS) {
        // Save into partitionMap
        partitionMap.get(partition).add(event);
      }

      // Add to ordered list
      msgs.add(event);

    }
    return msgs;
  }

  /**
   * Return a map containing one List of records per partition.
   * This internally creates a Kafka Consumer using the provided consumer properties.
   *
   * @param numPtns
   * @param consumerProperties
   * @return A Map of Partitions(Integer) and the resulting List of messages (byte[]) retrieved
   */
  public static Map<Integer, List<byte[]>> retrieveRecordsFromPartitions(String topic, int numPtns,
                                                                   Properties consumerProperties) {

    Map<Integer, List<byte[]>> resultsMap = new HashMap<Integer, List<byte[]>>();
    for (int i = 0; i < numPtns; i++) {
      List<byte[]> partitionResults = new ArrayList<byte[]>();
      resultsMap.put(i, partitionResults);
      KafkaConsumer<String, byte[]> consumer =
          new KafkaConsumer<String, byte[]>(consumerProperties);

      TopicPartition partition = new TopicPartition(topic, i);

      consumer.assign(Arrays.asList(partition));

      ConsumerRecords<String, byte[]> records = consumer.poll(1000);
      for (ConsumerRecord<String, byte[]> record : records) {
        partitionResults.add(record.value());
      }
      consumer.close();
    }
    return resultsMap;
  }

}
