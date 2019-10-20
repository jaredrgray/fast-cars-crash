package com.tesla.interview.application;

import static com.tesla.interview.application.ApplicationTools.logTrace;
import static java.lang.Math.ceil;
import static org.apache.logging.log4j.LogManager.getLogger;

import com.google.common.collect.Maps;
import com.tesla.interview.application.AsynchronousWriter.WriteTask;
import com.tesla.interview.io.MeasurementSampleReader;
import com.tesla.interview.model.AggregateSample;
import com.tesla.interview.model.IntegerHashtag;
import com.tesla.interview.model.MeasurementSample;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

public class InterviewApplication implements Callable<Void> {

  private static final Logger LOG = getLogger(InterviewApplication.class);

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

    int sum = 0;
    for (IntegerHashtag hashtag : measurement.getHashtags()) {
      sum += hashtag.getValue();
    }
    return new AggregateSample(sum, measurement.getId(), measurement.getPartitionNo(),
        measurement.getTimestamp());
  }

  final Map<Integer, Integer> partitionNoToThreadNo; // note: partitions indexed from 0
  final MeasurementSampleReader reader;
  final Map<Integer, AsynchronousWriter> threadNumToWriter;

  /**
   * Constructor.
   * 
   * @param numWriteThreads max. number of {@link Thread}s to dedicate towards writing output files
   * @param maxFileHandles max. number of file handles we should have open concurrently
   * @param outputFilePaths paths to the output samples files
   * @param inputFilePath path to the input samples file
   */
  public InterviewApplication(int numWriteThreads, int maxFileHandles, List<String> outputFilePaths,
      String inputFilePath) {

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
    /* END: validate input */

    this.reader = new MeasurementSampleReader(Paths.get(inputFilePath).toFile());
    this.partitionNoToThreadNo = Maps.newHashMap();
    this.threadNumToWriter = Maps.newHashMap();
    int maxPartitionsPerThread =
        (int) ceil(outputFilePaths.size() / Double.valueOf(numWriteThreads));

    // construct the reader and list of write threads from validated input
    for (int threadNo = 0; threadNo < numWriteThreads; threadNo++) {
      Map<Integer, String> partitionNoToPath = Maps.newHashMap();
      for (int partitionNo = threadNo * maxPartitionsPerThread; partitionNo < outputFilePaths.size()
          && partitionNo < (threadNo + 1) * maxPartitionsPerThread; partitionNo++) {
        partitionNoToPath.put(partitionNo + 1, outputFilePaths.get(partitionNo));
        partitionNoToThreadNo.put(partitionNo, threadNo);
      }
      AsynchronousWriter writer = new AsynchronousWriter(numWriteThreads, partitionNoToPath);
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
   */
  // @formatter:off
  InterviewApplication(
      Map<Integer, Integer> partitionNoToThreadNo,
      MeasurementSampleReader reader,
      Map<Integer, AsynchronousWriter> threadNumToWriter) {
    
    this.partitionNoToThreadNo = partitionNoToThreadNo;
    this.reader = reader;
    this.threadNumToWriter = threadNumToWriter;
  }
  // @formatter:on

  private static final Duration PRINT_INTERVAL = Duration.ofSeconds(3);

  @Override
  public Void call() {

    LOG.info("starting application");

    // spawn all writes
    LOG.info("spawning write tasks");
    Instant lastPrintTime = Instant.MIN;
    int spawnCount = 0;
    int lastSpawnCount = 0;
    Queue<Future<WriteTask>> q = new ArrayDeque<>();
    while (reader.hasNext()) {
      AggregateSample aggregate = aggregateMeasurement(reader.next());
      // convert from: index-from-one, to: index-from-zero
      int partitionNo = aggregate.getPartitionNo() - 1;
      int threadNo = partitionNoToThreadNo.getOrDefault(partitionNo, -1 /* defaultValue */);
      AsynchronousWriter writer = threadNumToWriter.getOrDefault(threadNo, null /* defaultValue */);
      if (writer != null) {
        q.add(writer.writeSample(aggregate));
        spawnCount++;
      } else {
        // if this happens, InterviewApplication is bugged!
        String message = String.format("No writer found -- partitionNo: %s, threadNo: %d",
            partitionNo, threadNo);
        LOG.fatal(message);
        throw new IllegalStateException(message);
      }

      // print status periodically
      if (Duration.between(lastPrintTime, Instant.now()).compareTo(PRINT_INTERVAL) > 0) {
        
        lastPrintTime = Instant.now();
        if (spawnCount > lastSpawnCount) {
          LOG.info(String.format("spawned new write tasks -- numSpawned: %d",
              spawnCount - lastSpawnCount));
        }
        lastSpawnCount = spawnCount;
      }
    }

    LOG.info(String.format("all write tasks spawned -- numSpawned: %d", q.size()));

    // wait for all writes to finish
    lastPrintTime = Instant.MIN;
    while (!q.isEmpty()) {
      Future<WriteTask> next = q.remove();
      try {
        next.get(10, TimeUnit.SECONDS); // TODO make timeout configurable
      } catch (InterruptedException e) {
        // no problem!
        Thread.currentThread().interrupt();
      } catch (TimeoutException e) {
        // minor problem!
        LOG.warn("Unexpected timeout while waiting for write thread -- message: " + e.getMessage());
      } catch (ExecutionException e) {
        // big problem!
        String message = "Unexpected exception while waiting for write thread -- " + " message: "
            + e.getMessage();
        LOG.error(message);
        logTrace(LOG, Level.ERROR, e);
      }

      // print status periodically
      if (Duration.between(lastPrintTime, Instant.now()).compareTo(PRINT_INTERVAL) > 0) {
        lastPrintTime = Instant.now();
        LOG.info(String.format("awaiting write task completion -- numRemaining: %d", q.size()));
      }
    }

    LOG.info("all write tasks have completed");

    LOG.info("stopping application");

    // tidy the room
    reader.close();
    for (AsynchronousWriter writer : threadNumToWriter.values()) {
      writer.close();
    }

    // all done!
    LOG.info("application stopped");
    return null;
  }
}
