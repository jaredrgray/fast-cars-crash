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

import static com.tesla.interview.io.AggregateSampleWriter.fromFile;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Charsets;
import com.tesla.interview.model.AggregateSample;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;

public class TestAggregateWriter {

  private static final String MUST_BE_A_NEW = "must be a new";
  private static final String UNEXPECTED_ERROR = "Unexpected error";

  @Test
  void testCloseAttemptsWithBadWriter() throws IOException {
    final String methodName = "testCloseFailsWithBadWriter";
    String filePrefix = String.format("%s_%s", getClass().getCanonicalName(), methodName);
    File file = Files.createTempFile(filePrefix, null /* suffix */).toFile();

    try {
      BufferedWriter writerSpy = spy(
          new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), Charsets.UTF_8)));
      doThrow(new IOException()).when(writerSpy).close();

      AggregateSampleWriter underTest = AggregateSampleWriter.withWriterMock(writerSpy);
      underTest.close();
      verify(writerSpy).close();

    } finally {
      assertTrue(file.delete());
    }
  }

  @Test
  void testCloseAttemptsWithGoodWriter() throws IOException {
    final String methodName = "testCloseFailsWithBadWriter";
    String filePrefix = String.format("%s_%s", getClass().getCanonicalName(), methodName);
    File file = Files.createTempFile(filePrefix, null /* suffix */).toFile();
    try {
      BufferedWriter writerSpy = spy(
          new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), Charsets.UTF_8)));

      AggregateSampleWriter underTest = AggregateSampleWriter.withWriterMock(writerSpy);
      underTest.close();
      verify(writerSpy).close();
    } finally {
      assertTrue(file.delete());
    }
  }

  @Test
  void testConstructorFailsWithExistingFile() {
    File fileMock = mock(File.class);
    when(fileMock.exists()).thenReturn(true);
    try {
      AggregateSampleWriter writer = fromFile(fileMock);
      writer.close();
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(MUST_BE_A_NEW));
    }
  }

  @Test
  void testConstructorFailsWithNullFile() {
    try {
      AggregateSampleWriter underTest = AggregateSampleWriter.fromFile(null /* fileToWrite */);
      underTest.close();
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("cannot be null"));
    }
  }

  @Test
  void testFromFileSucceedsWithNewFile() throws IOException {
    final String methodName = "testWriteSucceedsWithMultipleWrites";
    String filePrefix = String.format("%s_%s", getClass().getCanonicalName(), methodName);
    File file = Files.createTempFile(filePrefix, null /* suffix */).toFile();
    assertTrue(file.delete());

    AggregateSampleWriter underTest = null;
    try {
      underTest = AggregateSampleWriter.fromFile(file);
      assertTrue(file.exists());
    } finally {
      if (underTest != null) {
        underTest.close();
      }
      assertTrue(file.delete());
    }
  }

  @Test
  void testWriteFailsWithBadWriter() throws IOException {
    final String methodName = "testConstructorFailsWithGoofyFile";
    String filePrefix = String.format("%s_%s", getClass().getCanonicalName(), methodName);
    File file = Files.createTempFile(filePrefix, null /* suffix */).toFile();

    BufferedWriter writerSpy =
        spy(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), Charsets.UTF_8)));
    doThrow(new IOException()).when(writerSpy).write(anyString());
    AggregateSampleWriter underTest = AggregateSampleWriter.withWriterMock(writerSpy);

    try {
      underTest.writeSample(mock(AggregateSample.class));
      fail("expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(UNEXPECTED_ERROR));
    } finally {
      underTest.close();
      assertTrue(file.delete());
    }
  }

  @Test
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  void testWriteSucceedsWithGoodWriter() throws IOException {
    final String methodName = "testWriteSucceedsWithGoodWriter";
    String filePrefix = String.format("%s_%s", getClass().getCanonicalName(), methodName);
    File file = Files.createTempFile(filePrefix, null /* suffix */).toFile();

    BufferedWriter writerSpy =
        spy(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), Charsets.UTF_8)));
    AggregateSampleWriter underTest = AggregateSampleWriter.withWriterMock(writerSpy);
    AggregateSample sampleMock = mock(AggregateSample.class);

    String dataToWrite = "POOP";
    doReturn(dataToWrite).when(sampleMock).toString();
    underTest.writeSample(sampleMock);
    verify(writerSpy).write(eq(dataToWrite));

    underTest.close();
    assertTrue(file.delete());
  }

  @Test
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  void testWriteSucceedsWithMultipleWrites() throws IOException {
    final String methodName = "testWriteSucceedsWithMultipleWrites";
    String filePrefix = String.format("%s_%s", getClass().getCanonicalName(), methodName);
    File file = Files.createTempFile(filePrefix, null /* suffix */).toFile();
    try {
      BufferedWriter writerSpy = spy(
          new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), Charsets.UTF_8)));
      AggregateSampleWriter underTest = AggregateSampleWriter.withWriterMock(writerSpy);
      AggregateSample sampleMock = mock(AggregateSample.class);

      for (int i = 0; i < 10; i++) {
        String dataToWrite = "POOP" + i;
        doReturn(dataToWrite).when(sampleMock).toString();
        underTest.writeSample(sampleMock);
        verify(writerSpy).write(eq(dataToWrite));
      }

      underTest.close();
    } finally {
      assertTrue(file.delete());
    }
  }

}
