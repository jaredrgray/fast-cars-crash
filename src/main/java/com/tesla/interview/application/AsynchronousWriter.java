package com.tesla.interview.application;

import static org.apache.logging.log4j.LogManager.getLogger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tesla.interview.io.AggregateSampleWriter;
import com.tesla.interview.model.AggregateSample;
import java.io.Closeable;
import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Logger;

public class AsynchronousWriter implements Closeable {

  private static final Logger LOG = getLogger(AsynchronousWriter.class);

  final ExecutorService executor;
  final Map<Integer, String> partitionNoToPath;
  final Map<String, AggregateSampleWriter> pathToWriter;
  final List<AggregateSampleWriter> writers;

  /**
   * Constructor.
   * 
   * @param threadPoolSize number of threads for this writer
   * @param partitionNoToPath map from partition number to file system path
   */
  public AsynchronousWriter(int threadPoolSize, Map<Integer, String> partitionNoToPath) {
    if (threadPoolSize <= 0) {
      throw new IllegalArgumentException("threadPoolSize must be positive");
    }
    if (partitionNoToPath == null || partitionNoToPath.isEmpty()) {
      throw new IllegalArgumentException("partitionNoToPath cannot be empty");
    }

    this.executor = Executors.newFixedThreadPool(threadPoolSize);
    this.partitionNoToPath = partitionNoToPath;
    this.writers = Lists.newArrayList();
    this.pathToWriter = Maps.newHashMap();

    for (String path : partitionNoToPath.values()) {
      File file = Paths.get(path).toFile();
      if (file != null) {
        if (!pathToWriter.containsKey(path)) {
          AggregateSampleWriter writer = AggregateSampleWriter.fromFile(file);
          if (writer != null) {
            writers.add(writer);
            pathToWriter.put(path, writer);
          } else {
            throw new IllegalStateException("Unable to open file -- path: " + path);
          }
        } else {
          throw new IllegalArgumentException(
              "Cannot specify identical path more than once -- path: " + path);
        }
      } else {
        throw new IllegalArgumentException(
            "Unable to resolve file path for writing -- path: " + path);
      }
    }
  }

  /**
   * Injection constructor for unit tests.
   * 
   * @param executor executor to inject
   * @param partitionNoToPath maps partition numbers to paths of files
   * @param pathToWriter maps paths of files to writers
   * @param writers writers to inject
   */
  // @formatter:off
  AsynchronousWriter(
      ExecutorService executor,
      Map<Integer, String> partitionNoToPath,
      Map<String, AggregateSampleWriter> pathToWriter,
      List<AggregateSampleWriter> writers) {
    
    this.executor = executor;
    this.partitionNoToPath = partitionNoToPath;
    this.pathToWriter = pathToWriter;
    this.writers = writers;
  }
  // @formatter:on

  @Override
  public void close() {
    for (AggregateSampleWriter asw : pathToWriter.values()) {
      asw.close();
    }

    executor.shutdown();
    Duration waitDuration = Duration.ofSeconds(10); // TODO configurable timeout
    Instant endWaitTime = Instant.now().plusSeconds(waitDuration.getSeconds());

    while (Instant.now().isBefore(endWaitTime) && !executor.isTerminated()) {
      try {
        long waitTimeInMillis = Duration.between(Instant.now(), endWaitTime).toMillis() / 2;
        executor.awaitTermination(waitTimeInMillis, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (!executor.isTerminated()) {
      LOG.warn(String.format("Could not shut down executor service within %d %s",
          waitDuration.getSeconds(), TimeUnit.SECONDS.name()));
    }
  }

  /**
   * Write the aggregated sample to the output file.
   * 
   * @param sample aggregation to write
   * @return a progress indicator for the write
   */
  public Future<Void> writeSample(AggregateSample sample) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() {
        String path =
            partitionNoToPath.getOrDefault(sample.getPartitionNo(), null /* defaultValue */);
        AggregateSampleWriter writer = pathToWriter.getOrDefault(path, null /* defaultValue */);
        if (path != null && writer != null) {
          writer.writeSample(sample);
          return null /* success! */;
        } else {
          throw new IllegalArgumentException("Invalid path: " + path); // null-safe
        }
      }
    });
  }
}
