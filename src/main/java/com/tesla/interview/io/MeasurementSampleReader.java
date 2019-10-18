package com.tesla.interview.io;

import static org.apache.logging.log4j.LogManager.getLogger;

import com.google.common.io.Files;
import com.tesla.interview.model.MeasurementSample;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import org.apache.logging.log4j.Logger;

public class MeasurementSampleReader implements Closeable, Iterator<MeasurementSample> {

  private static final Logger LOG = getLogger(AggregateSampleWriter.class);

  private volatile boolean hasNext;
  private volatile int lineNo;
  private final String path;
  private final BufferedReader reader;

  /**
   * Constructor.
   * 
   * @param sampleFile file whose samples to read
   */
  public MeasurementSampleReader(File sampleFile) {
    if (sampleFile == null) {
      throw new IllegalArgumentException("sampleFile cannot be null");
    }
    if (!sampleFile.exists() || !sampleFile.isFile() || !sampleFile.canRead()) {
      throw new IllegalArgumentException("sampleFile must be a readable file");
    }

    try {
      reader = Files.newReader(sampleFile, StandardCharsets.UTF_8);
      reader.mark(1 /* readAheadLimit */);
      hasNext = reader.read() != -1;
      reader.reset();
    } catch (IOException e) {
      throw new IllegalStateException("Unexpected error while opening file", e);
    }

    this.lineNo = 1;
    this.path = sampleFile.getPath();
  }

  @Override
  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      String logMessage = String
          .format("Unexpected error while closing file -- filePath: %s, lineNo: %d", path, lineNo);
      LOG.error(logMessage);
    }
  }

  @Override
  public boolean hasNext() {
    return this.hasNext;
  }

  @Override
  public MeasurementSample next() {
    try {
      String nextLine = reader.readLine();
      this.lineNo++;
      return MeasurementSample.fromString(nextLine);
    } catch (Exception e) {
      throw new IllegalStateException("Unexpected error while reading file at line " + lineNo, e);
    }

  }
}
