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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

public class TestInteviewApplication {

  private static class Pair<T> {
    final T good;
    final T bad;

    Pair(T good, T bad) {
      this.good = good;
      this.bad = bad;
    }
  }
  
  private static final Logger LOG = getLogger(TestInteviewApplication.class);

  protected final Random rand = new Random(0xdeadbeef);

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
          Lists.newArrayList("valid"), "" /* inputFilePath */);
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

    String methodName = "testConstructorFailsOnInvalidInputs";
    for (int i = 0; i <= 1 << 4; i++) {
      boolean firstBitSet = (i >> 0) % 2 == 1;
      boolean secondBitSet = (i >> 1) % 2 == 1;
      boolean thirdBitSet = (i >> 2) % 2 == 1;
      boolean fourthBitSet = (i >> 3) % 2 == 1;
      LOG.info(String.format(
          "%s -- i: %d: firstBitSet: %s, secondBitSet: %s, thirdBitSet: %s, fourthBitSet: %s",
          methodName, i, firstBitSet, secondBitSet, thirdBitSet, fourthBitSet));

      try {
        new InterviewApplication(//
            firstBitSet ? numWriteThreads.bad : numWriteThreads.good,
            secondBitSet ? maxFileHandles.bad : maxFileHandles.good,
            thirdBitSet ? outputFilePaths.bad : outputFilePaths.good,
            fourthBitSet ? inputFilePath.bad : inputFilePath.good);
        fail("expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        assert (e.getMessage().contains("must"));
      }
    }
  }

  @Test
  void testConstructorHappyOnePartition() throws IOException {
    String methodName = "testConstructorHappyOnePartition";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);

    Path tempInputFile = Files.createTempFile(filePrefix, null /* suffix */);
    Path tempOutputFile = Files.createTempFile(filePrefix, null /* suffix */);
    assertTrue(tempOutputFile.toFile().delete());

    try {
      InterviewApplication underTest = new InterviewApplication(1 /* numWriteThreads */,
          1 /* maxFileHandles */, Lists.newArrayList(tempOutputFile.toString()),
          tempInputFile.toString() /* inputFilePath */);
      assertEquals(1, underTest.partitionNumToThreadNo.size());
    } finally {
      assertTrue(tempInputFile.toFile().delete());
    }
  }

  @Test
  void testConstructorHappyThreePartitions() throws IOException {
    String methodName = "testConstructorHappyThreePartitions";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);

    Path tempInputFile = Files.createTempFile(filePrefix, null /* suffix */);
    List<Path> tempOutputFiles = Lists.newArrayList(//
        Files.createTempFile(filePrefix, null /* suffix */),
        Files.createTempFile(filePrefix, null /* suffix */),
        Files.createTempFile(filePrefix, null /* suffix */));

    List<String> outputFilesAsStrings = Lists.newArrayList();
    for (Path p : tempOutputFiles) {
      outputFilesAsStrings.add(p.toString());
      assertTrue(p.toFile().delete());
    }

    try {
      int numWriteThreads = 1;
      InterviewApplication underTest =
          new InterviewApplication(numWriteThreads, numWriteThreads /* maxFileHandles */,
              outputFilesAsStrings, tempInputFile.toString() /* inputFilePath */);
      assertEquals(tempOutputFiles.size(), underTest.partitionNumToThreadNo.size());
      assertEquals(numWriteThreads, underTest.threadNumToWriter.size());
    } finally {
      assertTrue(tempInputFile.toFile().delete());
    }
  }

  @Test
  void testConstructorHappyThreeThreadsFivePartitions() throws IOException {
    String methodName = "testConstructorHappyThreePartitions";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);

    Path tempInputFile = Files.createTempFile(filePrefix, null /* suffix */);
    List<Path> tempOutputFiles = Lists.newArrayList(//
        Files.createTempFile(filePrefix, null /* suffix */),
        Files.createTempFile(filePrefix, null /* suffix */),
        Files.createTempFile(filePrefix, null /* suffix */),
        Files.createTempFile(filePrefix, null /* suffix */),
        Files.createTempFile(filePrefix, null /* suffix */));

    List<String> outputFilesAsStrings = Lists.newArrayList();
    for (Path p : tempOutputFiles) {
      outputFilesAsStrings.add(p.toString());
      assertTrue(p.toFile().delete());
    }

    try {
      int numWriteThreads = 3;
      InterviewApplication underTest =
          new InterviewApplication(numWriteThreads, numWriteThreads /* maxFileHandles */,
              outputFilesAsStrings, tempInputFile.toString() /* inputFilePath */);
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
        LOG.info(String.format("%s -- threadNum: %d, partitions: %s", methodName, threadNum,
            partitionNos));
      }

      assertEquals(2, threadNumToPartitionNos.get(0).size());
      assertEquals(2, threadNumToPartitionNos.get(1).size());
      assertEquals(1, threadNumToPartitionNos.get(2).size());

    } finally {
      assertTrue(tempInputFile.toFile().delete());
    }
  }

  @Test
  void testConstructorNegativeFileHandles() {
    try {
      new InterviewApplication(1 /* numWriteThreads */, -1 /* maxFileHandles */,
          Lists.newArrayList("valid"), "valid");
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must be positive"));
    }
  }

  @Test
  void testConstructorZeroFileHandles() {
    try {
      new InterviewApplication(1 /* numWriteThreads */, 0 /* maxFileHandles */,
          Lists.newArrayList("valid"), "valid");
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must be positive"));
    }
  }
}
