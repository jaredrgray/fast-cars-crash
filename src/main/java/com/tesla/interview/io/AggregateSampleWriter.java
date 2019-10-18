package com.tesla.interview.io;

import static org.apache.logging.log4j.LogManager.getLogger;

import com.google.common.io.Files;
import com.tesla.interview.model.AggregateSample;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.logging.log4j.Logger;

public class AggregateSampleWriter implements Closeable {

  private static final Logger LOG = getLogger(AggregateSampleWriter.class);

  private volatile int lineNo;
  private final String path;
  private final BufferedWriter writer;

  /**
   * Constructor.
   * 
   * @param fileToWrite the file to which we will write new samples
   */
  public AggregateSampleWriter(File fileToWrite) {
    if (fileToWrite == null) {
      throw new IllegalArgumentException("fileToWrite cannot be null");
    }
    if (fileToWrite.exists()) {
      throw new IllegalArgumentException("fileToWrite must be a new, writable file");
    }

    try {
      writer = Files.newWriter(fileToWrite, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException("Unexpected error while opening file", e);
    }

    this.lineNo = 0;
    this.path = fileToWrite.getPath();
  }

  @Override
  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      String logMessage = String
          .format("Unexpected error while closing file -- filePath: %s, lineNo: %d", path, lineNo);
      LOG.error(logMessage);
    }
  }

  /**
   * Write a new sample.
   * 
   * @param sample the sample to write.
   */
  public void writeSample(AggregateSample sample) {
    try {
      writer.append(sample.toString());
      writer.newLine();
      lineNo++;
    } catch (IOException e) {
      String exceptionMessage = String.format(
          "Unexpected error while writing to file -- filePath: %s, lineNo: %d", path, lineNo);
      throw new IllegalStateException(exceptionMessage, e);
    }
  }
}
