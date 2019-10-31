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

import static com.tesla.interview.tests.IntegrationTestSuite.INTEGRATION_TEST_TAG;
import static java.lang.Math.floorMod;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.tesla.interview.io.MeasurementSampleReader;
import com.tesla.interview.model.AggregateSample;
import com.tesla.interview.model.IntegerHashtag;
import com.tesla.interview.model.MeasurementSample;
import com.tesla.interview.tests.InterviewTestCase;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.mockito.stubbing.OngoingStubbing;

class InterviewApplicationIntegrationTest extends InterviewTestCase {

  /**
   * Tracks the order in which write tasks were added.
   */
  private static class AsynchronousWriterSpy extends AsynchronousWriter {
    Queue<String> ids = new ArrayDeque<>();

    public AsynchronousWriterSpy(int threadPoolSize, Map<Integer, String> partitionNoToPath) {
      super(threadPoolSize, partitionNoToPath, new CollectorRegistry());
    }

    @Override
    public Future<WriteTask> writeSample(AggregateSample sample) {
      Future<WriteTask> writeSample = super.writeSample(sample);
      ids.add(sample.getAssetId());
      return writeSample;
    }
  }

  private static final Random RAND;
  private static final int QUEUE_SIZE;
  private static final Duration POLL_DURATION;
  private static final Supplier<CollectorRegistry> REGISTRY_SUPPLIER;
  private static final URL METRICS_ENDPOINT;

  static {
    try {
      RAND = new Random(0xdeadbeef);
      QUEUE_SIZE = 100;
      POLL_DURATION = Duration.ofSeconds(1);
      REGISTRY_SUPPLIER = () -> new CollectorRegistry();
      METRICS_ENDPOINT = new URL("http://127.0.0.1:1234");
    } catch (MalformedURLException e) {
      throw new IllegalStateException("unexpected syntax error", e);
    }
  }

  /**
   * Build maps to inject for {@link InterviewApplication} construction.
   * 
   * @param numPartitions total number of partitions
   * @param numThreads number of threads
   * @param partitionNumToThreadNum map of partition number to thread number
   * @param threadNumToPartitions map of thread number to list of partitions
   */
  private void buildMaps(//
      int numPartitions, //
      int numThreads, //
      Map<Integer, Integer> partitionNumToThreadNum, //
      Map<Integer, List<Integer>> threadNumToPartitions) {

    int partitionNum = 0;
    while (partitionNum < numPartitions) {
      int threadNo = partitionNum % numThreads;
      List<Integer> partitions = threadNumToPartitions.getOrDefault(threadNo, Lists.newArrayList());
      partitions.add(partitionNum);
      threadNumToPartitions.put(threadNo, partitions);

      partitionNumToThreadNum.put(partitionNum, threadNo);
      partitionNum++;
    }
  }

  /**
   * Create a new {@link AsynchronousWriter} to inject for {@link InterviewApplication}
   * construction.
   * 
   * @param testInfo test metadata
   * @param threadNumToPartitions map of thread number to list of partitions
   * @param methodName name of calling method (for logging)
   * @param threadNum current thread number
   * @return created writer
   * @throws IOException if temporary file cannot be created
   */
  private AsynchronousWriterSpy createWriter(//
      TestInfo testInfo, //
      Map<Integer, List<Integer>> threadNumToPartitions, //
      String methodName, //
      int threadNum) throws IOException {

    List<Integer> partitionsForThread = threadNumToPartitions.get(threadNum);
    Map<Integer, String> partitionNumToPath = Maps.newHashMap();
    for (int partitionNum : partitionsForThread) {
      Path newFile = createTempFile(testInfo);
      assertTrue(newFile.toFile().delete());
      partitionNumToPath.put(partitionNum, newFile.toString());
    }
    AsynchronousWriterSpy writer =
        new AsynchronousWriterSpy(partitionsForThread.size(), partitionNumToPath);
    writer.startScheduler();
    return writer;
  }

  /**
   * Build a collection of randomized hash tags. Since the source of randomness is seeded, the
   * resulting collection should be constant across different executions of this test.
   * 
   * @return the randomized tags
   */
  private Set<IntegerHashtag> randoHashtags() {
    Set<IntegerHashtag> tags = Sets.newHashSet();
    IntegerHashtag[] values = IntegerHashtag.values();
    for (IntegerHashtag tag : values) {
      if (RAND.nextInt() % values.length == 0) {
        tags.add(tag);
      }
    }
    return tags;
  }

  /**
   * Ensure the mocked call to {@link MeasurementSampleReader#hasNext()} returns <code>true</code>
   * once for each sample. After that, it should return <code>false</code>.
   * 
   * @param numSamples number of samples to stub
   * @param mockReader mock to modify by reference
   */
  private void stubHasNext(int numSamples, MeasurementSampleReader mockReader) {
    OngoingStubbing<Boolean> w = when(mockReader.hasNext());
    for (int i = 0; i < numSamples; i++) {
      w = w.thenReturn(true);
    }
    w.thenReturn(false);
  }

  /**
   * Build a randomized list of {@link MeasurementSample}s.
   * <p/>
   * Ensure the mocked call to {@link MeasurementSampleReader#next()} returns the samples in the
   * correct order.
   * 
   * @param numSamples number of samples to stub
   * @param numPartitions total number of partitions
   * @param mockReader mock to modify by reference
   * @return the created samples
   */
  private List<MeasurementSample> stubNext(//
      int numSamples, //
      int numPartitions, //
      MeasurementSampleReader mockReader) {

    OngoingStubbing<MeasurementSample> whenNext = when(mockReader.next());

    List<MeasurementSample> created = Lists.newArrayList();
    for (int i = 0; i < numSamples; i++) {
      MeasurementSample rando = new MeasurementSample(// praise be the formatter
          RAND.nextLong(), // timestamp
          1 + floorMod(RAND.nextInt(), numPartitions), // partitionNum (note: indexed from one)
          String.valueOf(RAND.nextLong()), // id
          randoHashtags()); // hashtags
      whenNext = whenNext.thenReturn(rando);
      created.add(rando);
    }
    whenNext.thenThrow(new IllegalStateException("next() called too many times"));
    return created;
  }

  @RepeatedTest(10)
  @Tag(INTEGRATION_TEST_TAG)
  @SuppressFBWarnings()
  void testCallHappyIntegration(TestInfo testInfo) throws IOException {

    // set test parameters
    int numPartitions = 7;
    int numThreads = 3;
    int numSamples = 5000;

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
          createWriter(testInfo, threadNumToPartitions, methodName, threadNum));
    }

    // build and run the app
    InterviewApplication underTest =
        new InterviewApplication(partitionNumToThreadNo, mockReader, threadNoToWriter,
            Queues.newArrayDeque(), QUEUE_SIZE, POLL_DURATION, METRICS_ENDPOINT, REGISTRY_SUPPLIER);
    stubHasNext(numSamples, mockReader);
    List<MeasurementSample> ordered = stubNext(numSamples, numPartitions, mockReader);
    underTest.call();

    // verify all measurements were emitted in correct order
    for (int i = 0; i < ordered.size(); i++) {
      MeasurementSample expectedSample = ordered.get(i);
      int partitionNum = expectedSample.getPartitionNo() - 1; // our map is indexed from zero
      int expectedThreadNum = partitionNumToThreadNo.get(partitionNum);
      AsynchronousWriterSpy writer =
          (AsynchronousWriterSpy) threadNoToWriter.get(expectedThreadNum);
      String actualAssetId = writer.ids.poll();
      assertEquals(expectedSample.getAssetId(), actualAssetId);
    }
  }
}
