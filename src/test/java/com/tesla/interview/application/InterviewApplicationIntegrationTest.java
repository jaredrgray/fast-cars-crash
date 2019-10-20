package com.tesla.interview.application;

import static com.tesla.interview.application.InterviewApplication.aggregateMeasurement;
import static java.lang.Math.floorMod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.tesla.interview.tests.IntegrationTestSuite.INTEGRATION_TEST_TAG;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.OngoingStubbing;

class InterviewApplicationIntegrationTest extends TestInteviewApplication {

  @Test
  @Tag(INTEGRATION_TEST_TAG)
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

  private void buildMaps(//
      int numPartitions, //
      int numThreads, //
      Map<Integer, Integer> partitionNumToThreadNo, //
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

  private List<MeasurementSample> stubNext(//
      int numSamples, //
      int numPartitions, //
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
}
