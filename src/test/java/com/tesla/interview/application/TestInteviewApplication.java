/*
 * Copyright (c) 2019 Jared R Gray
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tesla.interview.application;

import static com.tesla.interview.application.InterviewApplication.aggregateMeasurement;
import static org.apache.logging.log4j.LogManager.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tesla.interview.model.AggregateSample;
import com.tesla.interview.model.IntegerHashtag;
import com.tesla.interview.model.MeasurementSample;
import com.tesla.interview.tests.InterviewTestCase;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class TestInteviewApplication extends InterviewTestCase {

  private static class Pair<T> {
    final T good;
    final T bad;

    Pair(T good, T bad) {
      this.good = good;
      this.bad = bad;
    }
  }

  private static final Logger LOG;
  private static final int VALID_QUEUE_SIZE;
  private static final Duration VALID_POLL_DURATION;
  private static final URI VALID_ENDPOINT;
  private static final Supplier<CollectorRegistry> REGISTRY_SUPPLIER;

  static {
    try {
      LOG = getLogger(TestInteviewApplication.class);
      VALID_QUEUE_SIZE = 1;
      VALID_POLL_DURATION = Duration.ofSeconds(1);
      VALID_ENDPOINT = new URI("http://127.0.0.1:1234");
      REGISTRY_SUPPLIER = () -> new CollectorRegistry();
    } catch (URISyntaxException e) {
      throw new IllegalStateException("unexpected syntax error");
    }
  }

  @Test
  void testAggregateMeasurementNegative() {
    try {
      aggregateMeasurement(null /* measurement */);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("cannot be null"));
    }
  }

  @Test
  void testAggregateMeasurementPositive() {
    int timestamp = 0;
    int partitionNo = 1;
    String id = "2";
    MeasurementSample measurement = new MeasurementSample(timestamp, partitionNo, id,
        Sets.newHashSet(IntegerHashtag.FOUR, IntegerHashtag.TEN));

    AggregateSample agg = aggregateMeasurement(measurement);
    assertNotNull(agg);
    assertEquals(timestamp, agg.getTimestamp());
    assertEquals(partitionNo, agg.getPartitionNo());
    assertEquals(id, agg.getAssetId());
    assertEquals(14, agg.getAggregateValue());
  }

  @Test
  void testConstructorEmptyInputFile() {
    try {
      new InterviewApplication(1 /* numWriteThreads */, 1 /* maxFileHandles */,
          Lists.newArrayList("valid"), "" /* inputFilePath */, VALID_QUEUE_SIZE,
          VALID_POLL_DURATION, VALID_ENDPOINT, REGISTRY_SUPPLIER);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must be non-empty"));
    }
  }

  @Test
  @SuppressFBWarnings("IM_BAD_CHECK_FOR_ODD")
  void testConstructorFailsOnInvalidInputs() throws IOException {

    /*
     * Individually, some of these parameters are valid. In any and all combinations, however, no
     * input is completely valid.
     */
    Pair<Integer> numWriteThreads = new Pair<>(2, 0);
    Pair<Integer> maxFileHandles = new Pair<>(3, 1);
    Pair<List<String>> outputFilePaths =
        new Pair<>(Lists.newArrayList("valid1"), Lists.newArrayList());
    Pair<String> inputFilePath = new Pair<>("valid", null /* invalid */);
    Pair<Integer> queueSize = new Pair<>(10, -1);
    Pair<Duration> pollDuration = new Pair<>(VALID_POLL_DURATION, null);
    Pair<URI> metricsEndpoint = new Pair<>(VALID_ENDPOINT, null);
    Pair<Supplier<CollectorRegistry>> metricsRegistry = new Pair<>(REGISTRY_SUPPLIER, null);

    String methodName = "testConstructorFailsOnInvalidInputs";
    int numBits = 8;
    for (int i = 0; i <= 1 << (numBits - 1); i++) {
      boolean firstBitSet = (i >> 0) % 2 == 1;
      boolean secondBitSet = (i >> 1) % 2 == 1;
      boolean thirdBitSet = (i >> 2) % 2 == 1;
      boolean fourthBitSet = (i >> 3) % 2 == 1;
      boolean fifthBitSet = (i >> 4) % 2 == 1;
      boolean sixthBitSet = (i >> 5) % 2 == 1;
      boolean seventhBitSet = (i >> 6) % 2 == 1;
      boolean eigthBitSet = (i >> 7) % 2 == 1;
      LOG.info(String.format(
          "%s -- i: %d: firstBitSet: %s, secondBitSet: %s, thirdBitSet: %s, fourthBitSet: %s, ",
          methodName, i, firstBitSet, secondBitSet, thirdBitSet, fourthBitSet)
          + String.format("fifthBitSet: %s, sixthBitSet: %s, seventhBitSet: %s, eigthBitSet: %s",
              fifthBitSet, sixthBitSet, seventhBitSet, eigthBitSet));

      try {
        new InterviewApplication(//
            firstBitSet ? numWriteThreads.bad : numWriteThreads.good,
            secondBitSet ? maxFileHandles.bad : maxFileHandles.good,
            thirdBitSet ? outputFilePaths.bad : outputFilePaths.good,
            fourthBitSet ? inputFilePath.bad : inputFilePath.good,
            fifthBitSet ? queueSize.bad : queueSize.good,
            sixthBitSet ? pollDuration.bad : pollDuration.good,
            seventhBitSet ? metricsEndpoint.bad : metricsEndpoint.good,
            eigthBitSet ? metricsRegistry.bad : metricsRegistry.good);
        fail("expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        assert (e.getMessage().contains("must"));
      }
    }
  }

  @Test
  void testConstructorHappyOnePartition(TestInfo testInfo) throws IOException {
    Path tempInputFile = createTempFile(testInfo);
    Path tempOutputFile = createTempFile(testInfo);
    assertTrue(tempOutputFile.toFile().delete());

    InterviewApplication underTest = new InterviewApplication(1 /* numWriteThreads */,
        1 /* maxFileHandles */, Lists.newArrayList(tempOutputFile.toString()),
        tempInputFile.toString() /* inputFilePath */, VALID_QUEUE_SIZE, VALID_POLL_DURATION,
        VALID_ENDPOINT, REGISTRY_SUPPLIER);
    assertEquals(1, underTest.partitionNumToThreadNo.size());
  }

  @Test
  void testConstructorHappyThreePartitions(TestInfo testInfo) throws IOException {
    Path tempInputFile = createTempFile(testInfo);
    List<Path> tempOutputFiles = Lists.newArrayList(createTempFile(testInfo),
        createTempFile(testInfo), createTempFile(testInfo));

    List<String> outputFilesAsStrings = Lists.newArrayList();
    for (Path p : tempOutputFiles) {
      outputFilesAsStrings.add(p.toString());
      assertTrue(p.toFile().delete());
    }
    int numWriteThreads = 1;
    InterviewApplication underTest =
        new InterviewApplication(numWriteThreads, numWriteThreads /* maxFileHandles */,
            outputFilesAsStrings, tempInputFile.toString() /* inputFilePath */, VALID_QUEUE_SIZE,
            VALID_POLL_DURATION, VALID_ENDPOINT, REGISTRY_SUPPLIER);
    assertEquals(tempOutputFiles.size(), underTest.partitionNumToThreadNo.size());
    assertEquals(numWriteThreads, underTest.threadNumToWriter.size());
  }

  @Test
  void testConstructorHappyThreeThreadsFivePartitions(TestInfo testInfo) throws IOException {

    Path tempInputFile = createTempFile(testInfo);
    List<Path> tempOutputFiles =
        Lists.newArrayList(createTempFile(testInfo), createTempFile(testInfo),
            createTempFile(testInfo), createTempFile(testInfo), createTempFile(testInfo));

    List<String> outputFilesAsStrings = Lists.newArrayList();
    for (Path p : tempOutputFiles) {
      outputFilesAsStrings.add(p.toString());
      assertTrue(p.toFile().delete());
    }

    int numWriteThreads = 3;
    InterviewApplication underTest =
        new InterviewApplication(numWriteThreads, numWriteThreads /* maxFileHandles */,
            outputFilesAsStrings, tempInputFile.toString() /* inputFilePath */, VALID_QUEUE_SIZE,
            VALID_POLL_DURATION, VALID_ENDPOINT, REGISTRY_SUPPLIER);
    assertEquals(tempOutputFiles.size(), underTest.partitionNumToThreadNo.size());
    assertEquals(numWriteThreads, underTest.threadNumToWriter.size());

    Map<Integer, List<Integer>> threadNumToPartitionNos = Maps.newHashMap();
    HashSet<Integer> distinctValues = Sets.newHashSet(underTest.partitionNumToThreadNo.values());
    for (int threadNum : distinctValues) {
      assertTrue(threadNum < numWriteThreads);
      List<Integer> partitionNos = Lists.newArrayList();
      for (int partitionNum : underTest.partitionNumToThreadNo.keySet()) {
        if (underTest.partitionNumToThreadNo.get(partitionNum) == threadNum) {
          partitionNos.add(partitionNum);
        }
      }
      threadNumToPartitionNos.put(threadNum, partitionNos);
      LOG.info(String.format("%s -- threadNum: %d, partitions: %s",
          testInfo.getTestMethod().get().getName(), threadNum, partitionNos));
    }

    assertEquals(2, threadNumToPartitionNos.get(0).size());
    assertEquals(2, threadNumToPartitionNos.get(1).size());
    assertEquals(1, threadNumToPartitionNos.get(2).size());

  }

  @Test
  void testConstructorNegativeFileHandles() {
    try {
      new InterviewApplication(1 /* numWriteThreads */, -1 /* maxFileHandles */,
          Lists.newArrayList("valid"), "valid", VALID_QUEUE_SIZE, VALID_POLL_DURATION,
          VALID_ENDPOINT, REGISTRY_SUPPLIER);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must be positive"));
    }
  }

  @Test
  void testConstructorZeroFileHandles() {
    try {
      new InterviewApplication(1 /* numWriteThreads */, 0 /* maxFileHandles */,
          Lists.newArrayList("valid"), "valid", VALID_QUEUE_SIZE, VALID_POLL_DURATION,
          VALID_ENDPOINT, REGISTRY_SUPPLIER);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must be positive"));
    }
  }
}
