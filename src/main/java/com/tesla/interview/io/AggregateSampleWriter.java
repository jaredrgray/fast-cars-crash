package com.tesla.interview.io;

import static org.apache.logging.log4j.LogManager.getLogger;

import com.google.common.io.Files;
import com.tesla.interview.model.AggregateSample;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.logging.log4j.Logger;

public class AggregateSampleWriter implements Closeable {

  private static final Logger LOG = getLogger(AggregateSampleWriter.class);

  /**
   * Create an aggregate writer from a file.
   * 
   * @param fileToWrite file to which we will write new samples
   */
  public static AggregateSampleWriter fromFile(File fileToWrite) {
    if (fileToWrite == null) {
      throw new IllegalArgumentException("fileToWrite cannot be null");
    }
    if (fileToWrite.exists()) {
      throw new IllegalArgumentException("fileToWrite must be a new, writable file");
    }

    try {
      BufferedWriter writer = Files.newWriter(fileToWrite, StandardCharsets.UTF_8);
      return new AggregateSampleWriter(writer, 0 /* lineNo */, fileToWrite.getPath());
    } catch (FileNotFoundException e) {
      throw new IllegalStateException("Unexpected error while opening file", e);
    }
  }
  
  /**
   * Create a custom writer for unit testing.
   * 
   * @param mock mock or stub of writer
   * @return custom instance with mock and/or stub injected
   */
  public static AggregateSampleWriter withWriterMock(BufferedWriter mock) {
    return new AggregateSampleWriter(mock, -1 /* lineno */, null /* path */);
  }
  
  private int lineNo;
  private String path;
  private BufferedWriter writer;

  private AggregateSampleWriter(BufferedWriter writer, int lineNo, String path) {
    this.writer = writer;
    this.lineNo = lineNo;
    this.path = path;
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
