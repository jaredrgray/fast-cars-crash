package com.tesla.interview.application;

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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAsynchronousWriter {

  private static final String CANNOT_BE_EMPTY = "cannot be empty";
  private static final String MUST_BE_POSITIVE = "must be positive";

  private List<Path> filesToWrite;
  private Map<Integer, String> partitionToPath;
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
      Instant s = Instant.now();
      
      /* performance note: creating the first mock takes about one full second */
      AggregateSampleWriter writerMock = mock(AggregateSampleWriter.class);
      System.out.println(Duration.between(s, Instant.now()).toString());
      allWriters.add(writerMock);
      pathToWriter.put(p.toString(), writerMock);
    }
  }

  @AfterEach
  void afterEach() {
    if (underTest != null) {
      underTest.close();
    }

    for (int i = 0; i < filesToWrite.size(); i++) {
      File createdFile = filesToWrite.get(i).toFile();
      if (createdFile.exists()) {
        createdFile.delete();
      }
    }
  }

  @BeforeEach
  void beforeEach() throws IOException {
    filesToWrite = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      File createdFile = File.createTempFile(getClass().getName(), null /* suffix */);
      createdFile.delete();
      filesToWrite.add(Paths.get(createdFile.getPath()));
    }

    partitionToPath = Maps.newHashMap();
    int fileNo = 0;
    while (fileNo < filesToWrite.size()) {
      String nextPath = filesToWrite.get(fileNo).toString();
      partitionToPath.put(fileNo, nextPath);
      fileNo++;
    }

    bufferQueue = Queues.newArrayDeque();
    pathToWriter = Maps.newHashMap();
    maxWaitDuration = Duration.ofMillis(10);
    bufferSize = 10;
    pollDelay = Duration.ofMillis(1);
  }

  @Test
  void testCloseFailPath() {

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    underTest = new AsynchronousWriter(executorService, partitionToPath, pathToWriter,
        allWriters, bufferQueue, maxWaitDuration, pollDelay, bufferSize);

    // submit a task that will not complete before close timeout
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        Duration spinDuration = maxWaitDuration.plus(Duration.ofMillis(10));
        Instant end = Instant.now().plus(spinDuration);
        try {
          while (Instant.now().isBefore(end)) {
            Thread.sleep(spinDuration.toMillis() / 10);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    underTest.close();
    assertTrue(executorService.isShutdown());
    assertFalse(executorService.isTerminated());
  }

  @Test
  void testCloseFastPath() {
    underTest = new AsynchronousWriter(1 /* threadPoolSize */, partitionToPath);
    underTest.close();
  }

  @Test
  void testCloseSlowPath() {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });

    underTest = new AsynchronousWriter(executorService, partitionToPath, pathToWriter,
        allWriters, bufferQueue, maxWaitDuration, pollDelay, bufferSize);
    underTest.close();
  }

  @Test
  void testConstructorPositive() {
    underTest = new AsynchronousWriter(1 /* threadPoolSize */, partitionToPath);
  }

  @Test
  void testConstructorWithDuplicatePathsFails() {
    Map<Integer, String> customMap = Maps.newHashMap();
    customMap.putAll(partitionToPath);

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
      underTest = new AsynchronousWriter(0 /* threadPoolSize */, partitionToPath);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(MUST_BE_POSITIVE));
    }
  }

  @Test
  void testConstructorWithMissingFileFails() throws IOException {
    Map<Integer, String> customMap = Maps.newHashMap();
    customMap.putAll(partitionToPath);

    Path tempDir = Files.createTempDirectory(null /* prefix */);
    tempDir.toFile().delete();
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
      underTest = new AsynchronousWriter(-1 /* threadPoolSize */, partitionToPath);
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
    underTest = new AsynchronousWriter(executorService, partitionToPath, pathToWriter,
        allWriters, bufferQueue, maxWaitDuration, pollDelay, bufferSize);

    int partitionNo = 2;
    AggregateSample sample = new AggregateSample(0 /* aggregateValue */, "1" /* id */,
        partitionNo + 1, 3 /* timestamp */);
    Future<WriteTask> f = underTest.writeSample(sample);
    f.get();

    String writtenPath = partitionToPath.get(partitionNo);
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
    underTest = new AsynchronousWriter(executorService, partitionToPath, pathToWriter,
        allWriters, bufferQueue, maxWaitDuration, maxWaitDuration, bufferSize);

    int numPartitions = 3;
    List<Future<WriteTask>> futures = Lists.newArrayList();
    List<AggregateSample> samples = Lists.newArrayList();
    for (int partitionNo = 0; partitionNo < numPartitions; partitionNo++) {
      AggregateSample sample = new AggregateSample(partitionNo /* aggregateValue */,
          UUID.randomUUID().toString() /* id */, partitionNo + 1, partitionNo + 2 /* timestamp */);
      samples.add(sample);
      futures.add(underTest.writeSample(sample));
    }

    for (Future<WriteTask> f : futures) {
      f.get();
    }

    for (int partitionNo = 0; partitionNo < numPartitions; partitionNo++) {
      String writtenPath = partitionToPath.get(partitionNo);
      AggregateSampleWriter expectedWriter = pathToWriter.get(writtenPath);
      for (AggregateSampleWriter writer : allWriters) {
        AggregateSample s = samples.get(partitionNo);
        if (writer == expectedWriter) {
          verify(expectedWriter).writeSample(eq(s));
        } else {
          verify(writer, never()).writeSample(eq(s));
        }
      }
    }
  }
}
