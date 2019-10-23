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

import static com.tesla.interview.application.ApplicationTools.logTrace;
import static org.apache.logging.log4j.LogManager.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.tesla.interview.application.AsynchronousWriter.WriteTask;
import com.tesla.interview.io.AggregateSampleWriter;
import com.tesla.interview.model.AggregateSample;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAsynchronousWriter {

  private static final String CANNOT_BE_EMPTY = "cannot be empty";
  private static final String MUST_BE_POSITIVE = "must be positive";
  private static final Logger LOG = getLogger(TestAsynchronousWriter.class);

  private Stack<Path> filesToWrite;
  private Map<Integer, String> partitionNumToPath;
  private AsynchronousWriter underTest;
  private Map<String, AggregateSampleWriter> pathToWriter;
  private List<AggregateSampleWriter> allWriters;
  private Queue<WriteTask> bufferQueue;
  private Duration maxWaitDuration;
  private Duration pollDelay;
  private int bufferSize;

  private void createWriters() {
    allWriters = Lists.newArrayList();
    for (Path p : filesToWrite) {
      /* performance note: creating the first mock takes about one full second */
      AggregateSampleWriter writerMock = mock(AggregateSampleWriter.class);
      allWriters.add(writerMock);
      pathToWriter.put(p.toString(), writerMock);
    }
  }

  @AfterEach
  void afterEach() {
    if (underTest != null) {
      underTest.close();
    }

    while (!filesToWrite.empty()) {
      File createdFile = filesToWrite.pop().toFile();
      if (createdFile.exists()) {
        assertTrue(createdFile.delete());
      }
    }
  }

  @BeforeEach
  void beforeEach() throws IOException {
    filesToWrite = new Stack<>();
    for (int i = 0; i < 10; i++) {
      File createdFile = File.createTempFile(getClass().getName(), null /* suffix */);
      assertTrue(createdFile.delete());
      filesToWrite.add(Paths.get(createdFile.getPath()));
    }

    partitionNumToPath = Maps.newHashMap();
    int fileNo = 0;
    Iterator<Path> fileToWrite = filesToWrite.iterator();
    while (fileToWrite.hasNext()) {
      String nextPath = fileToWrite.next().toString();
      partitionNumToPath.put(fileNo, nextPath);
      fileNo++;
    }

    bufferQueue = Queues.newArrayDeque();
    pathToWriter = Maps.newHashMap();
    maxWaitDuration = Duration.ofMillis(10);
    bufferSize = 10;
    pollDelay = Duration.ofMillis(1);
  }

  @Test
  @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
  void testCloseFailPath() throws InterruptedException, ExecutionException {

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    underTest = new AsynchronousWriter(executorService, partitionNumToPath, pathToWriter,
        allWriters, bufferQueue, maxWaitDuration, pollDelay, bufferSize);
    underTest.startScheduler();

    // submit a task that will not complete before close() timeout
    Callable<Void> task = new Callable<Void>() {
      @Override
      public Void call() {
        Duration spinDuration = maxWaitDuration.plus(Duration.ofMillis(300));
        Instant end = Instant.now().plus(spinDuration);
        try {
          while (Instant.now().isBefore(end)) {
            Thread.sleep(spinDuration.toMillis() / 10);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return null;
      }
    };

    Future<Void> future = executorService.submit(task);
    underTest.close();
    assertTrue(executorService.isShutdown());
    assertFalse(executorService.isTerminated());
    future.get();
  }

  @Test
  void testCloseSuccessPath() {
    createWriters();
    underTest = new AsynchronousWriter(Executors.newSingleThreadExecutor(), partitionNumToPath,
        pathToWriter, allWriters, bufferQueue, maxWaitDuration, pollDelay, bufferSize);
    underTest.startScheduler();
    underTest.close();

    assertTrue(underTest.isClosed.get());
    assertFalse(underTest.scheduler.isAlive());
    assertTrue(underTest.executor.isShutdown());
    assertTrue(underTest.executor.isTerminated());
    for (AggregateSampleWriter asw : underTest.pathToWriter.values()) {
      verify(asw).close();
    }
  }

  @Test
  @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
  @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
  void testCloseExecutorFailPath()
      throws InterruptedException, BrokenBarrierException, ExecutionException {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    CyclicBarrier barrier = new CyclicBarrier(2);
    Callable<Void> task = new Callable<Void>() {
      @Override
      public Void call() {
        try {
          barrier.await();
          return null;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        } catch (BrokenBarrierException e) {
          logTrace(LOG, Level.ERROR, e);
          throw new IllegalStateException("Unexpected exception");
        }
      }
    };

    Future<Void> future = executorService.submit(task);
    underTest = new AsynchronousWriter(executorService, partitionNumToPath, pathToWriter,
        allWriters, bufferQueue, maxWaitDuration, pollDelay, bufferSize);
    underTest.startScheduler();
    underTest.close();

    assertTrue(underTest.isClosed.get());
    assertFalse(underTest.scheduler.isAlive());
    assertTrue(underTest.executor.isShutdown());
    assertFalse(underTest.executor.isTerminated());
    barrier.await();
    future.get();
  }

  @Test
  void testConstructorPositive() {
    underTest = new AsynchronousWriter(1 /* threadPoolSize */, partitionNumToPath);
    underTest.startScheduler();
    assertEquals(1, underTest.bufferSize);
    assertTrue(underTest.scheduler.isAlive());
  }

  @Test
  void testConstructorWithDuplicatePathsFails() {
    Map<Integer, String> customMap = Maps.newHashMap();
    customMap.putAll(partitionNumToPath);

    String dupPath = customMap.get(1);
    customMap.put(2, dupPath);

    try {
      underTest = new AsynchronousWriter(1 /* threadPoolSize */, customMap);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot specify identical path"));
    }
  }

  @Test
  void testConstructorWithEmptyMapFails() {
    try {
      underTest = new AsynchronousWriter(1 /* threadPoolSize */, Maps.newHashMap());
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(CANNOT_BE_EMPTY));
    }
  }

  @Test
  void testConstructorWithEmptyPoolFails() {
    try {
      underTest = new AsynchronousWriter(0 /* threadPoolSize */, partitionNumToPath);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(MUST_BE_POSITIVE));
    }
  }

  @Test
  void testConstructorWithMissingFileFails() throws IOException {
    Map<Integer, String> customMap = Maps.newHashMap();
    customMap.putAll(partitionNumToPath);

    Path tempDir = Files.createTempDirectory(null /* prefix */);
    filesToWrite.push(tempDir);
    assertTrue(tempDir.toFile().delete());
    String pathWithNoFile = tempDir.resolve("iDoNotExist").toString();
    customMap.put(1, pathWithNoFile);

    try {
      underTest = new AsynchronousWriter(1 /* threadPoolSize */, customMap);
      fail("expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Unable to open"));
    }
  }

  @Test
  void testConstructorWithNegativePoolSizeFails() {
    try {
      underTest = new AsynchronousWriter(-1 /* threadPoolSize */, partitionNumToPath);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(MUST_BE_POSITIVE));
    }
  }

  @Test
  void testConstructorWithNullMapFails() {
    try {
      underTest = new AsynchronousWriter(1 /* threadPoolSize */, null /* partitionNoToPath */);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(CANNOT_BE_EMPTY));
    }
  }

  @Test
  void testWriteSamplePositiveOnePartition() throws InterruptedException, ExecutionException {
    createWriters();
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    underTest = new AsynchronousWriter(executorService, partitionNumToPath, pathToWriter,
        allWriters, bufferQueue, maxWaitDuration, pollDelay, bufferSize);
    underTest.startScheduler();

    // build the tasks and fire them off
    int partitionNo = 2;
    AggregateSample sample = new AggregateSample(0 /* aggregateValue */, "1" /* id */,
        partitionNo + 1, 3 /* timestamp */);
    Future<WriteTask> f = underTest.writeSample(sample);
    f.get();

    // verify that correct ASW processed the request
    String writtenPath = partitionNumToPath.get(partitionNo);
    AggregateSampleWriter spyWriter = pathToWriter.get(writtenPath);
    for (AggregateSampleWriter spy : allWriters) {
      if (spy.equals(spyWriter)) {
        verify(spyWriter).writeSample(eq(sample));
      } else {
        verify(spy, never()).writeSample(any(AggregateSample.class));
      }
    }
  }

  @Test
  void testWriteSamplePositiveThreePartitions() throws InterruptedException, ExecutionException {
    createWriters();
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    underTest = new AsynchronousWriter(executorService, partitionNumToPath, pathToWriter,
        allWriters, bufferQueue, maxWaitDuration, maxWaitDuration, bufferSize);
    underTest.startScheduler();

    // build the tasks and fire them off
    int numPartitions = 3;
    List<Future<WriteTask>> futures = Lists.newArrayList();
    List<AggregateSample> samples = Lists.newArrayList();
    for (int partitionNum = 0; partitionNum < numPartitions; partitionNum++) {
      AggregateSample sample = new AggregateSample(partitionNum /* aggregateValue */,
          UUID.randomUUID().toString() /* assetId */, partitionNum + 1,
          partitionNum + 2 /* timestamp */);
      samples.add(sample);
      futures.add(underTest.writeSample(sample));
    }

    // wait for all tasks to execute
    for (Future<WriteTask> f : futures) {
      f.get();
    }

    // verify that correct ASW processed the request
    for (int partitionNum = 0; partitionNum < numPartitions; partitionNum++) {
      String writtenPath = partitionNumToPath.get(partitionNum);
      AggregateSampleWriter expectedWriter = pathToWriter.get(writtenPath);
      for (AggregateSampleWriter writer : allWriters) {
        AggregateSample s = samples.get(partitionNum);
        if (writer == expectedWriter) {
          verify(expectedWriter).writeSample(eq(s));
        } else {
          verify(writer, never()).writeSample(eq(s));
        }
      }
    }
  }
}
