package com.tesla.interview.io;

import com.google.common.io.Files;
import com.tesla.interview.model.MeasurementSample;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class SampleReader implements Closeable, Iterator<MeasurementSample> {

  private final BufferedReader reader;

  private volatile boolean hasNext;
  private volatile int lineNo = 1;

  /**
   * Constructor.
   * 
   * @param sampleFile file whose samples to read
   */
  public SampleReader(File sampleFile) {
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
      throw new IllegalStateException("Unexpected error while reading file", e);
    }
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
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
