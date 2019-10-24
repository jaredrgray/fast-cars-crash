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

/**
 * Writes a series of output text files, where each line of output is a String representation of an
 * {@link AggregateSample}.
 */
public class AggregateSampleWriter implements Closeable {

  private static final Logger LOG = getLogger(AggregateSampleWriter.class);

  /**
   * Create an aggregate writer from a file. Classes in outside packages should use this in lieu of
   * a constructor.
   * 
   * @param fileToWrite file to which we will write new samples
   * @return non-<code>null</code> {@link AggregateSampleWriter}
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
      throw new IllegalStateException(
          String.format("Unable to open output file -- reason: %s, path: %s", "no such file",
              fileToWrite.getPath(), e));
    }
  }

  /**
   * Injection for unit testing.
   * 
   * @param mock mock or stub of writer
   * @return custom instance with mock and/or stub injected
   */
  static AggregateSampleWriter withWriterMock(BufferedWriter mock) {
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
      LOG.error(String.format("Unexpected error while closing file -- filePath: %s, lineNo: %d",
          path, lineNo));
    }
  }

  /**
   * Write a new sample to the associated output file.
   * 
   * @param sample the sample to write.
   */
  public void writeSample(AggregateSample sample) {
    try {
      writer.append(sample.toString());
      writer.newLine();
      lineNo++;
    } catch (IOException e) {
      throw new IllegalStateException(String.format(
          "Unexpected error while writing to file -- filePath: %s, lineNo: %d", path, lineNo), e);
    }
  }
}
