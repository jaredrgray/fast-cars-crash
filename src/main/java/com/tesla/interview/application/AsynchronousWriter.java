package com.tesla.interview.application;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tesla.interview.io.AggregateSampleWriter;
import com.tesla.interview.model.AggregateSample;
import java.io.Closeable;
import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AsynchronousWriter implements Closeable {

  private final ExecutorService executor;
  private final Map<Integer, String> partitionNoToPath;
  private final Map<String, AggregateSampleWriter> pathToWriter;
  private final List<AggregateSampleWriter> writers;

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
        AggregateSampleWriter writer = new AggregateSampleWriter(file);
        writers.add(writer);
        AggregateSampleWriter existing = pathToWriter.put(path, writer);
        if (existing != null) {
          throw new IllegalArgumentException(
              "Cannot specify identical path more than once -- path: " + path);
        }
      } else {
        throw new IllegalArgumentException(
            "Unable to resolve file path for writing -- path: " + path);
      }
    }
  }

  @Override
  public void close() {
    for (AggregateSampleWriter asw : pathToWriter.values()) {
      asw.close();
    }
  }

  /**
   * Write the aggregated sample to the output file.
   * 
   * @param sample aggregation to write
   * @return a progress indicator for the write
   */
  public Future<Void> writeSample(AggregateSample sample) {
    Callable<Void> r = new Callable<Void>() {
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
    };
    return executor.submit(r);
  }
}
