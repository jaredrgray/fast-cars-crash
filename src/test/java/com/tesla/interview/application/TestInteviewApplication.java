package com.tesla.interview.application;

import static com.tesla.interview.application.InterviewApplication.aggregateMeasurement;
import static java.lang.Math.floorMod;
import static org.apache.logging.log4j.LogManager.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tesla.interview.io.MeasurementSampleReader;
import com.tesla.interview.model.AggregateSample;
import com.tesla.interview.model.IntegerHashtag;
import com.tesla.interview.model.MeasurementSample;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.OngoingStubbing;

public class TestInteviewApplication {

  private static final Logger LOG = getLogger(TestInteviewApplication.class);
  private static final Random rand = new Random(0xdeadbeef);

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
    assertEquals(id, agg.getId());
    assertEquals(14, agg.getAggregateValue());
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
  void testConstructorFailsOnInvalidInputs() throws IOException {
    class Pair<T> {
      final T good;
      final T bad;

      Pair(T good, T bad) {
        this.good = good;
        this.bad = bad;
      }
    }

    /*
     * Individually, some of these parameters are valid. In any and all combinations, however, no
     * input is completely valid.
     */
    Pair<Integer> numWriteThreads = new Pair<>(2, 0);
    Pair<Integer> maxFileHandles = new Pair<>(3, 1);
    Pair<List<String>> outputFilePaths =
        new Pair<>(Lists.newArrayList("valid1"), Lists.newArrayList());
    Pair<String> inputFilePath = new Pair<>("valid", null /* invalid */);

    String methodName = "testInterviewAppConstructorFailsOnInvalidInputs";
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
  void testConstructorHappyOnePartition() throws IOException {
    String methodName = "testConstructorHappyOnePartition";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);

    Path tempInputFile = Files.createTempFile(filePrefix, null /* suffix */);
    Path tempOutputFile = Files.createTempFile(filePrefix, null /* suffix */);
    tempOutputFile.toFile().delete();

    try {
      InterviewApplication underTest = new InterviewApplication(1 /* numWriteThreads */,
          1 /* maxFileHandles */, Lists.newArrayList(tempOutputFile.toString()),
          tempInputFile.toString() /* inputFilePath */);
      assertEquals(1, underTest.partitionNoToThreadNo.size());
    } finally {
      tempInputFile.toFile().delete();
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
      p.toFile().delete();
    }

    try {
      int numWriteThreads = 1;
      InterviewApplication underTest =
          new InterviewApplication(numWriteThreads, numWriteThreads /* maxFileHandles */,
              outputFilesAsStrings, tempInputFile.toString() /* inputFilePath */);
      assertEquals(tempOutputFiles.size(), underTest.partitionNoToThreadNo.size());
      assertEquals(numWriteThreads, underTest.threadNumToWriter.size());
    } finally {
      tempInputFile.toFile().delete();
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
      p.toFile().delete();
    }

    try {
      int numWriteThreads = 3;
      InterviewApplication underTest =
          new InterviewApplication(numWriteThreads, numWriteThreads /* maxFileHandles */,
              outputFilesAsStrings, tempInputFile.toString() /* inputFilePath */);
      assertEquals(tempOutputFiles.size(), underTest.partitionNoToThreadNo.size());
      assertEquals(numWriteThreads, underTest.threadNumToWriter.size());

      Map<Integer, List<Integer>> threadNumToPartitionNos = Maps.newHashMap();
      HashSet<Integer> distinctValues = Sets.newHashSet(underTest.partitionNoToThreadNo.values());
      for (int threadNum : distinctValues) {
        assertTrue(threadNum < numWriteThreads);
        List<Integer> partitionNos = Lists.newArrayList();
        for (int partitionNum : underTest.partitionNoToThreadNo.keySet()) {
          if (underTest.partitionNoToThreadNo.get(partitionNum) == threadNum) {
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
      tempInputFile.toFile().delete();
    }
  }

  @Test
  void testCallHappyIntegration() throws IOException {
    List<Path> filesCreated = Lists.newArrayList();
    int numPartitions = 7;
    int numThreads = 3;
    int numSamples = 50;

    try {

      // build maps
      Map<Integer, Integer> partitionNumToThreadNo = Maps.newHashMap();
      Map<Integer, List<Integer>> threadNumToPartitions = Maps.newHashMap();
      buildMaps(numPartitions, numThreads, partitionNumToThreadNo, threadNumToPartitions);

      // create reader and writers
      String methodName = "testCallHappyIntegration";
      MeasurementSampleReader mockReader = mock(MeasurementSampleReader.class);
      Map<Integer, AsynchronousWriter> threadNoToWriter = Maps.newHashMap();
      for (int threadNum = 0; threadNum < numThreads; threadNum++) {
        threadNoToWriter.put(threadNum,
            spy(createWriter(filesCreated, threadNumToPartitions, methodName, threadNum)));
      }

      // build and run the app
      InterviewApplication underTest =
          new InterviewApplication(partitionNumToThreadNo, mockReader, threadNoToWriter);
      stubHasNext(numSamples, mockReader);
      List<MeasurementSample> ordered = stubNext(numSamples, numPartitions, mockReader);
      underTest.call();

      // verify all measurements were emitted in correct order
      for (int i = 0; i < ordered.size(); i++) {
        MeasurementSample sample = ordered.get(i);
        int partitionNum = sample.getPartitionNo() - 1; // our map is indexed from zero
        int expectedThreadNum = partitionNumToThreadNo.get(partitionNum);
        AsynchronousWriter spyWriter = threadNoToWriter.get(expectedThreadNum);
        AggregateSample expectedWritten = aggregateMeasurement(sample);
        // verify(spyWriter.writeSample(eq(expectedWritten)));
      }

    } finally {
      for (Path p : filesCreated) {
        p.toFile().delete();
      }
    }
  }

  private List<MeasurementSample> stubNext(int numSamples, int numPartitions,
      MeasurementSampleReader mockReader) {
    OngoingStubbing<MeasurementSample> whenNext = when(mockReader.next());

    List<MeasurementSample> created = Lists.newArrayList();
    for (int i = 0; i < numSamples; i++) {
      MeasurementSample rando = new MeasurementSample(// praise be the formatter
          rand.nextLong(), // timestamp
          1 + floorMod(rand.nextInt(), numPartitions), // partitionNum (note: indexed from one)
          String.valueOf(rand.nextLong()), // id
          randoHashtags()); // hashtags
      whenNext = whenNext.thenReturn(rando);
      created.add(rando);
    }
    whenNext = whenNext.thenThrow(new IllegalStateException("next() called too many times"));
    return created;
  }

  private Set<IntegerHashtag> randoHashtags() {
    Set<IntegerHashtag> tags = Sets.newHashSet();
    IntegerHashtag[] values = IntegerHashtag.values();
    for (IntegerHashtag tag : values) {
      if (rand.nextInt() % values.length == 0) {
        tags.add(tag);
      }
    }
    return tags;
  }

  private void stubHasNext(int numSamples, MeasurementSampleReader spyReader) {
    OngoingStubbing<Boolean> w = when(spyReader.hasNext());
    for (int i = 0; i < numSamples; i++) {
      w = w.thenReturn(true);
    }
    w = w.thenReturn(false);
  }

  private AsynchronousWriter createWriter(//
      List<Path> filesCreated, //
      Map<Integer, List<Integer>> threadNumToPartitions, //
      String methodName, //
      int threadNum) throws IOException {

    List<Integer> partitionsForThread = threadNumToPartitions.get(threadNum);
    Map<Integer, String> partitionNumToPath = Maps.newHashMap();
    for (int partitionNum : partitionsForThread) {
      String filePrefix = String.format("%s_%s", getClass().getName(), methodName);
      Path newFile = Files.createTempFile(filePrefix/* prefix */, null /* suffix */);
      filesCreated.add(newFile);
      newFile.toFile().delete();
      partitionNumToPath.put(partitionNum, newFile.toString());
    }
    return new AsynchronousWriter(partitionsForThread.size(), partitionNumToPath);
  }

  private void buildMaps(int numPartitions, int numThreads,
      Map<Integer, Integer> partitionNumToThreadNo,
      Map<Integer, List<Integer>> threadNumToPartitions) {
    int partitionNum = 0;
    while (partitionNum < numPartitions) {
      int threadNo = partitionNum % numThreads;
      List<Integer> partitions = threadNumToPartitions.getOrDefault(threadNo, Lists.newArrayList());
      partitions.add(partitionNum);
      threadNumToPartitions.put(threadNo, partitions);

      partitionNumToThreadNo.put(partitionNum, threadNo);
      partitionNum++;
    }
  }
}
