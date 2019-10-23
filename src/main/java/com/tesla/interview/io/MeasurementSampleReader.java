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
import com.tesla.interview.model.MeasurementSample;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.logging.log4j.Logger;

/**
 * Reads a series of {@link MeasurementSample} lines from an input text file.
 */
public class MeasurementSampleReader implements Closeable, Iterator<MeasurementSample> {

  private static final Logger LOG = getLogger(MeasurementSampleReader.class);

  /**
   * Allow unit tests to mock the buffered reader.
   * 
   * @param reader mocked reader to inject
   * @return instance with mocked reader injected
   */
  static MeasurementSampleReader withMockedReader(BufferedReader reader) {
    return new MeasurementSampleReader(1 /* lineNo */, null /* path */, reader);
  }

  private int lineNo;
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
      throw new IllegalArgumentException("sampleFile must be an existing readable file");
    }

    try {
      reader = Files.newReader(sampleFile, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException("Unexpected error while opening file", e);
    }

    this.lineNo = 1;
    this.path = sampleFile.getPath();
  }

  private MeasurementSampleReader(int lineNo, String path, BufferedReader reader) {
    this.lineNo = lineNo;
    this.path = path;
    this.reader = reader;
  }

  @Override
  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      LOG.error(String.format("Unexpected error while closing file -- filePath: %s, lineNo: %d",
          path, lineNo));
    }
  }

  @Override
  public boolean hasNext() {
    try {
      reader.mark(1 /* readAheadLimit */);
      boolean hasNext = reader.read() != -1;
      reader.reset();
      return hasNext;
    } catch (IOException e) {
      String exceptionMessage = String
          .format("Unexpected error while reading file -- filePath: %s, lineNo: %d", path, lineNo);
      throw new IllegalStateException(exceptionMessage, e);
    }
  }

  @Override
  public MeasurementSample next() {
    if (hasNext()) {
      try {
        String nextLine = reader.readLine();
        this.lineNo++;
        return MeasurementSample.fromString(nextLine);
      } catch (IOException e) {
        throw new IllegalStateException(String.format(
            "Unexpected error while reading file -- filePath: %s, lineNo: %d", path, lineNo), e);
      }
    } else {
      throw new NoSuchElementException();
    }

  }
}
