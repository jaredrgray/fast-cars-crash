package com.tesla.interview.application;

import static java.lang.Math.ceil;
import static org.apache.logging.log4j.LogManager.getLogger;

import com.google.common.collect.Maps;
import com.tesla.interview.io.MeasurementSampleReader;
import com.tesla.interview.model.AggregateSample;
import com.tesla.interview.model.IntegerHashtag;
import com.tesla.interview.model.MeasurementSample;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.Logger;

public class InterviewApplication implements Callable<Void> {

  private static final Logger LOG = getLogger(InterviewApplication.class);

  /**
   * Construct an aggregate from a sample.
   * 
   * @param measurement sample to aggregate
   * @return the aggregation
   */
  public static AggregateSample aggregateMeasurement(MeasurementSample measurement) {
    int sum = 0;
    for (IntegerHashtag hashtag : measurement.getHashtags()) {
      sum += hashtag.getValue();
    }
    return new AggregateSample(sum, measurement.getId(), measurement.getPartitionNo(),
        measurement.getTimestamp());
  }

  private final Map<Integer, Integer> partitionNoToThreadNo; // note: partitions indexed from 0
  private final MeasurementSampleReader reader;
  private final Map<Integer, AsynchronousWriter> threadNoToWriter;

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
    if (maxFileHandles <= numWriteThreads) {
      throw new IllegalArgumentException("maxFileHandles must be at least numWriteThreads");
    }
    if (outputFilePaths == null || outputFilePaths.isEmpty()) {
      throw new IllegalArgumentException("outputFilePaths must be non-empty");
    }
    if (outputFilePaths.size() <= numWriteThreads) {
      throw new IllegalArgumentException(
          "outputFilePaths must contain at least numWriteThreads paths");
    }
    if (inputFilePath == null || inputFilePath.isEmpty()) {
      throw new IllegalArgumentException("inputFilePath must be non-empty");
    }
    /* END: validate input */

    this.reader = new MeasurementSampleReader(Paths.get(inputFilePath).toFile());
    this.partitionNoToThreadNo = Maps.newHashMap();
    this.threadNoToWriter = Maps.newHashMap();
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
      threadNoToWriter.put(threadNo, writer);
    }

    // from construction onwards, our maps are immutable (i.e. thread-safe)!
  }

  @Override
  public Void call() {
    // spawn all writes
    Queue<Future<Void>> q = new ArrayDeque<>();
    while (reader.hasNext()) {
      MeasurementSample nextSample = reader.next();
      AggregateSample aggregate = aggregateMeasurement(nextSample);
      int theadNo =
          partitionNoToThreadNo.getOrDefault(aggregate.getPartitionNo(), -1 /* defaultValue */);
      AsynchronousWriter writer = threadNoToWriter.getOrDefault(theadNo, null /* defaultValue */);
      if (writer != null) {
        q.add(writer.writeSample(aggregate));
      }
    }

    // wait for all writes
    while (!q.isEmpty()) {
      Future<Void> next = q.remove();
      try {
        next.get(10, TimeUnit.SECONDS); // TODO make configurable
      } catch (InterruptedException e) {
        // no problem!
        Thread.currentThread().interrupt();
      } catch (TimeoutException e) {
        // minor problem!
        LOG.warn("Unexpected timeout while waiting for write thread -- message: " + e.getMessage());
      } catch (ExecutionException e) {
        // big problem!
        LOG.error(
            "Unexpected exception while waiting for write thread -- message: " + e.getMessage());
      }
    }

    // close down all readers and writers
    reader.close();
    for (AsynchronousWriter writer : threadNoToWriter.values()) {
      writer.close();
    }

    // all done!
    return null;
  }
}
