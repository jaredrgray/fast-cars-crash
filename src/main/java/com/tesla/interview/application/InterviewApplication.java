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
import static java.lang.Math.ceil;
import static java.lang.Math.floorMod;
import static org.apache.logging.log4j.LogManager.getLogger;

import com.google.common.collect.Maps;
import com.tesla.interview.application.AsynchronousWriter.WriteTask;
import com.tesla.interview.io.MeasurementSampleReader;
import com.tesla.interview.model.AggregateSample;
import com.tesla.interview.model.MeasurementSample;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

/**
 * Parses an input file into a series of {@link MeasurementSample}s, aggregate them, then executes
 * a series of {@link WriteTask}s to write the resulting aggregation to an output file.
 */
public class InterviewApplication implements Callable<Void> {

  private static final String THREAD_NUM = "thread_num";
  private static final String INSTANCE = "instance";
  private static final String JOB_NAME = "inteview_application_call";

  /**
   * Wait for all write tasks spawned by the {@link InterviewApplication#producer}.
   */
  class TaskConsumer implements Callable<Void> {

    @Override
    public Void call() throws Exception {
      awaitWrites();
      return null;
    }

    /**
     * Wait for all writes to complete.
     */
    private void awaitWrites() {

      // wait for each write
      LOG.info("waiting for in-flight writes to complete");
      Instant nextPrintTime = Instant.MIN;
      Future<WriteTask> next;
      int numCompleted = 0;
      while ((next = consumeWrite()) != null) {
        waitForWrite(next);
        numCompleted++;

        // print status periodically
        if (Instant.now().isAfter(nextPrintTime)) {
          nextPrintTime = randomizedPrintTime();
          LOG.info(
              String.format("awaiting write task completion -- numCompleted: %s", numCompleted));
        }
      }

      // wait for the last batch
      taskLock.lock();
      try {
        while (!pendingTasks.isEmpty()) {
          next = pendingTasks.poll();
          queueHasRoom.signal();
          waitForWrite(next);
        }
      } finally {
        taskLock.unlock();
      }
      LOG.info("all write tasks have completed");
    }

    /**
     * Consume and return the next scheduled {@link WriteTask} from the buffer.
     * 
     * @return next buffered write in the queue or <code>null</code> if read phase of application is
     *         complete
     */
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private Future<WriteTask> consumeWrite() {
      Future<WriteTask> task = null;
      taskLock.lock();
      try {
        while (pendingTasks.isEmpty() && !readComplete.get()) {
          try {
            if (LOG.isEnabled(Level.DEBUG)) {
              LOG.debug("START -- sampleAvailable.await()");
            }
            sampleAvailable.await(pollDuration.toMillis(), TimeUnit.MILLISECONDS);
            if (LOG.isEnabled(Level.DEBUG)) {
              LOG.debug("END   -- sampleAvailable.await()");
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        if (!pendingTasks.isEmpty()) {
          task = pendingTasks.remove();
          queueHasRoom.signal();
        }
      } finally {
        taskLock.unlock();
      }
      return task;
    }

    /**
     * Wait until the scheduled write has completed.
     * 
     * @param write write for which to wait
     */
    private void waitForWrite(Future<WriteTask> write) {
      try {
        if (LOG.isEnabled(Level.DEBUG)) {
          LOG.debug("START -- write.get()");
        }
        write.get();
        if (LOG.isEnabled(Level.DEBUG)) {
          LOG.debug("END   -- write.get()");
        }
      } catch (InterruptedException e) {
        // no problem!
      } catch (ExecutionException e) {
        // big problem!
        String message = "Unexpected exception while waiting for write thread -- " + " message: "
            + e.getMessage();
        LOG.error(message);
        logTrace(LOG, Level.ERROR, e);
      }
    }
  }

  /**
   * Spawns write tasks for {@link AsynchronousWriter}.
   */
  class TaskProducer implements Callable<Void> {

    @Override
    public Void call() throws Exception {
      spawnWrites();
      return null;
    }

    /**
     * Schedule the next write from the input file.
     * 
     * @param aggregate write to schedule
     * @param writer writer that will execute the write
     */
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private void spawnWrite(AggregateSample aggregate, AsynchronousWriter writer) {
      taskLock.lock();
      try {
        while (pendingTasks.size() == maxNumTasks) {
          try {
            if (LOG.isEnabled(Level.DEBUG)) {
              LOG.debug("START -- queueHasRoom.await()");
            }
            queueHasRoom.await(pollDuration.toMillis(), TimeUnit.MILLISECONDS);
            if (LOG.isEnabled(Level.DEBUG)) {
              LOG.debug("END   -- queueHasRoom.await()");
            }
          } catch (InterruptedException e) {
            // we were deliberately interrupted; check condition again
          }
        }
        Future<WriteTask> future = writer.writeSample(aggregate);
        pendingTasks.add(future);
        sampleAvailable.signal();
      } finally {
        taskLock.unlock();
      }
    }

    /**
     * Spawn the full series of {@link WriteTask}s based on data gleaned from the input file.
     */
    private void spawnWrites() {

      // read next sample
      LOG.info("spawning write tasks");
      Instant nextPrintTime = Instant.MIN;
      int spawnCount = 0;
      while (reader.hasNext()) {
        AggregateSample aggregate = aggregateMeasurement(reader.next());

        // associate sample with correct writer
        int partitionNo = aggregate.getPartitionNo() - 1;
        int threadNo = partitionNumToThreadNo.getOrDefault(partitionNo, -1 /* defaultValue */);
        AsynchronousWriter writer =
            threadNumToWriter.getOrDefault(threadNo, null /* defaultValue */);
        if (writer != null) {

          // enqueue write
          spawnWrite(aggregate, writer);
          spawnCount++;
        } else {

          // if this happens, InterviewApplication is bugged!
          String message = String.format("No writer found -- partitionNo: %s, threadNo: %d",
              partitionNo, threadNo);
          LOG.fatal(message);
          throw new IllegalStateException(message);
        }

        // print status periodically
        if (Instant.now().isAfter(nextPrintTime)) {
          nextPrintTime = randomizedPrintTime();
          LOG.info(String.format("spawning new write tasks -- numSpawned: %d", spawnCount));
        }
      }

      // finish up
      if (!readComplete.compareAndSet(false, true)) {
        throw new IllegalStateException("read cannot complete more than once");
      }
      LOG.info(String.format("all write tasks spawned -- numSpawned: %d", spawnCount));
    }
  }

  private static final Logger LOG = getLogger(InterviewApplication.class);
  private static final Duration PRINT_INTERVAL = Duration.ofSeconds(3); // TODO make configurable
  private static final Random RANDOM = new Random();

  /**
   * Construct an aggregate from a sample.
   * <p/>
   * Package-visible for unit tests.
   * 
   * @param measurement sample to aggregate
   * @return the aggregation
   */
  static AggregateSample aggregateMeasurement(MeasurementSample measurement) {
    if (measurement == null) {
      throw new IllegalArgumentException("measurement cannot be null");
    }

    int sum = measurement.getHashtags().stream().mapToInt((ht) -> ht.getValue()).sum();
    return new AggregateSample(sum, measurement.getAssetId(), measurement.getPartitionNo(),
        measurement.getTimestamp());
  }

  private final Lock taskLock = new ReentrantLock();
  private final Condition sampleAvailable = taskLock.newCondition();
  private final Condition queueHasRoom = taskLock.newCondition();
  private final AtomicBoolean readComplete = new AtomicBoolean(false /* initialValue */);
  private final TaskConsumer consumer = new TaskConsumer();
  private final TaskProducer producer = new TaskProducer();
  private final ExecutorService executor = Executors.newFixedThreadPool(2 /* nThreads */);
  private final Map<String, CollectorRegistry> threadNameToRegistry;
  private final String instanceId;

  final Map<Integer, Integer> partitionNumToThreadNo; // note: partitions indexed from 0
  final MeasurementSampleReader reader;
  final int maxNumTasks;
  final Map<Integer, AsynchronousWriter> threadNumToWriter;
  final Queue<Future<WriteTask>> pendingTasks;
  final Duration pollDuration;
  final URL metricsEndpoint;
  final Supplier<CollectorRegistry> registrySupplier;

  /**
   * Canonical constructor.
   * 
   * @param numWriteThreads max. number of {@link Thread}s to dedicate towards writing output files
   * @param maxFileHandles max. number of file handles we should have open concurrently
   * @param outputFilePaths paths to the output samples files
   * @param inputFilePath path to the input samples file
   * @param queueSize size of write queue
   * @param pollDuration max. amount of time to wait between polls
   */
  public InterviewApplication(int numWriteThreads, int maxFileHandles, List<String> outputFilePaths,
      String inputFilePath, int queueSize, Duration pollDuration, URL metricsEndpoint,
      Supplier<CollectorRegistry> registrySupplier) {

    /* BEGIN: validate input */
    if (numWriteThreads <= 0) {
      throw new IllegalArgumentException("numWriteThreads must be positive");
    }
    if (maxFileHandles <= 0) {
      throw new IllegalArgumentException("maxFileHandles must be positive");
    }
    if (maxFileHandles < numWriteThreads) {
      throw new IllegalArgumentException("maxFileHandles must be at least numWriteThreads");
    }
    if (outputFilePaths == null || outputFilePaths.isEmpty()) {
      throw new IllegalArgumentException("outputFilePaths must be non-empty");
    }
    if (outputFilePaths.size() < numWriteThreads) {
      throw new IllegalArgumentException(
          "outputFilePaths must contain at least numWriteThreads paths");
    }
    if (inputFilePath == null || inputFilePath.isEmpty()) {
      throw new IllegalArgumentException("inputFilePath must be non-empty");
    }
    if (queueSize <= 0) {
      throw new IllegalArgumentException("queueSize must be positive");
    }
    if (pollDuration == null) {
      throw new IllegalArgumentException("pollDuration cannot be null");
    }
    /* END: validate input */

    this.reader = new MeasurementSampleReader(Paths.get(inputFilePath).toFile());
    this.partitionNumToThreadNo = Maps.newHashMap();
    this.threadNumToWriter = Maps.newHashMap();
    this.maxNumTasks = queueSize;
    this.pendingTasks = new ArrayDeque<Future<WriteTask>>(maxNumTasks);
    this.pollDuration = pollDuration;
    this.metricsEndpoint = metricsEndpoint;
    this.registrySupplier = registrySupplier;
    this.instanceId = UUID.randomUUID().toString().replace("-", "g");
    this.threadNameToRegistry = Maps.newHashMap();

    // construct the reader and list of write threads from validated input
    int maxPartitionsPerThread =
        (int) ceil(outputFilePaths.size() / Double.valueOf(numWriteThreads));
    for (int threadNo = 0; threadNo < numWriteThreads; threadNo++) {
      Map<Integer, String> partitionNumToPath = Maps.newHashMap();
      for (int partitionNo = threadNo * maxPartitionsPerThread; partitionNo < outputFilePaths.size()
          && partitionNo < (threadNo + 1) * maxPartitionsPerThread; partitionNo++) {
        String ourPath = outputFilePaths.get(partitionNo);

        // delete the existing files if there are conflicts
        // TODO make this behavior configurable via parameter flag
        File ourFile = Paths.get(ourPath).toFile();
        if (ourFile != null && ourFile.exists()) {
          if (!ourFile.delete()) {
            throw new IllegalStateException(
                String.format("Failed to delete file -- path: %s", ourFile.getPath()));
          }
        }

        // maintain maps
        partitionNumToPath.put(partitionNo, ourPath);
        partitionNumToThreadNo.put(partitionNo, threadNo);
      }
      
      // prepare metric emission
      CollectorRegistry metricsRegistry = registrySupplier.get();
      threadNameToRegistry.put(String.valueOf(threadNo), metricsRegistry);
      
      // build writers
      AsynchronousWriter writer =
          new AsynchronousWriter(numWriteThreads, partitionNumToPath, metricsRegistry);
      writer.startScheduler();
      threadNumToWriter.put(threadNo, writer);
    }

    // from construction onwards, our maps are immutable (i.e. thread-safe)!
  }

  /**
   * Injection constructor for unit tests.
   * 
   * @param partitionNoToThreadNo injected mapping of partitions to threads
   * @param reader injected sample reader
   * @param threadNumToWriter injected mapping of threads to writers
   * @param taskQueue queue of write tasks
   * @param maxQueueSize maximum queue size
   * @param pollDuration max. time to wait between polls
   */
  InterviewApplication(//
      Map<Integer, Integer> partitionNoToThreadNo, //
      MeasurementSampleReader reader, //
      Map<Integer, AsynchronousWriter> threadNumToWriter, //
      Queue<Future<WriteTask>> taskQueue, //
      int maxQueueSize, //
      Duration pollDuration, //
      URL metricsEndpoint, //
      Supplier<CollectorRegistry> registrySupplier) {

    this.partitionNumToThreadNo = partitionNoToThreadNo;
    this.reader = reader;
    this.threadNumToWriter = threadNumToWriter;
    this.pendingTasks = taskQueue;
    this.maxNumTasks = maxQueueSize;
    this.pollDuration = pollDuration;
    this.metricsEndpoint = metricsEndpoint;
    this.registrySupplier = registrySupplier;
    this.threadNameToRegistry = Maps.newHashMap();
    this.instanceId = "instanceId";
  }

  @Override
  public Void call() {

    // spawn threads
    LOG.info("starting application");
    Future<Void> producerFuture = executor.submit(producer);
    Future<Void> consumerFuture = executor.submit(consumer);

    // wait till we're done
    try {
      producerFuture.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw new IllegalStateException("unexpected error", e);
    }
    try {
      consumerFuture.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw new IllegalStateException("unexpected error", e);
    }

    // tidy up
    LOG.info("stopping application");
    reader.close();
    executor.shutdownNow();
    for (AsynchronousWriter writer : threadNumToWriter.values()) {
      writer.close();
    }

    // emit all metrics
    if (metricsEndpoint != null) {
      String urlToString = urlToString(metricsEndpoint);
      PushGateway pg = new PushGateway(urlToString);
      for (Entry<String, CollectorRegistry> entry : threadNameToRegistry.entrySet()) {
        try {
          Map<String, String> groupingKey = Maps.newHashMap();
          groupingKey.put(INSTANCE, instanceId);
          groupingKey.put(THREAD_NUM, entry.getKey());
          pg.pushAdd(entry.getValue(), JOB_NAME, groupingKey);
        } catch (IOException e) {
          throw new IllegalStateException("failed to emit metrics", e);
        }
      }
    }

    // all done!
    LOG.info("application stopped");
    return null;
  }

  private static String urlToString(URL url) {
    return String.format("%s:%s%s", url.getHost(), url.getPort(), url.getPath());
  }

  private Instant randomizedPrintTime() {
    Instant nextPrintTime;
    Duration randomizedDuration =
        Duration.ofMillis(floorMod(RANDOM.nextLong(), PRINT_INTERVAL.toMillis()));
    nextPrintTime = Instant.now().plus(randomizedDuration);
    return nextPrintTime;
  }
}
