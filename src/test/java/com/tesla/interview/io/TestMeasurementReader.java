package com.tesla.interview.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tesla.interview.model.IntegerHashtag;
import com.tesla.interview.model.MeasurementSample;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class TestMeasurementReader {

  private static final String UNEXPECTED_ERROR_WHILE_READING = "Unexpected error while reading";

  @Test
  void testConstructorSucceedsWithExistingFile() throws IOException {
    String methodName = "testConstructorSucceedsWithExistingFile";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);
    File sampleFile = Files.createTempFile(filePrefix, null /* suffix */).toFile();

    MeasurementSampleReader underTest = null;
    try {
      underTest = new MeasurementSampleReader(sampleFile);
    } finally {
      if (underTest != null) {
        underTest.close();
      }
      sampleFile.delete();
    }
  }

  @Test
  void testConstructorFailsWhenFileIsActuallyDirectory() throws IOException {
    String methodName = "testConstructorFailsWhenFileIsActuallyDirectory";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);
    File sampleFile = Files.createTempDirectory(filePrefix).toFile();

    MeasurementSampleReader underTest = null;
    try {
      underTest = new MeasurementSampleReader(sampleFile);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must be an existing"));
    } finally {
      if (underTest != null) {
        underTest.close();
      }
      sampleFile.delete();
    }
  }

  @Test
  void testConstructorFailsWhenFileIsNull() throws IOException {
    MeasurementSampleReader underTest = null;
    try {
      underTest = new MeasurementSampleReader(null /* sampleFile */);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("cannot be null"));
    } finally {
      if (underTest != null) {
        underTest.close();
      }
    }
  }

  @Test
  void testCloseWhenSuccessful() throws IOException {
    String methodName = "testCloseWhenSuccessful";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);
    File sampleFile = Files.createTempFile(filePrefix, null /* suffix */).toFile();

    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(sampleFile)));
    BufferedReader readerSpy = spy(reader);
    try {
      MeasurementSampleReader underTest = MeasurementSampleReader.withMockedReader(readerSpy);
      underTest.close();
      verify(readerSpy).close();
    } finally {
      sampleFile.delete();
    }
  }

  @Test
  void testCloseWhenUnsuccessful() throws IOException {
    String methodName = "testCloseWhenUnsuccessful";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);
    File sampleFile = Files.createTempFile(filePrefix, null /* suffix */).toFile();

    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(sampleFile)));
    BufferedReader readerSpy = spy(reader);
    doThrow(new IOException()).when(readerSpy).close();

    try {
      MeasurementSampleReader underTest = MeasurementSampleReader.withMockedReader(readerSpy);
      underTest.close();
      verify(readerSpy).close();
    } finally {
      sampleFile.delete();
    }
  }

  @Test
  void testReadsOneLine() throws IOException {
    String methodName = "testReadsOneLine";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);
    File sampleFile = Files.createTempFile(filePrefix, null /* suffix */).toFile();

    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(sampleFile)));
    BufferedReader readerSpy = spy(reader);

    String sampleId = UUID.randomUUID().toString();
    Set<IntegerHashtag> tags = Sets.newHashSet(IntegerHashtag.FIVE);
    MeasurementSample sample =
        new MeasurementSample(0 /* timestamp */, 1 /* partitionNo */, sampleId, tags);
    when(readerSpy.read()).thenReturn(1).thenReturn(-1);
    doReturn(sample.toString()).when(readerSpy).readLine();

    MeasurementSampleReader underTest = null;
    try {
      underTest = MeasurementSampleReader.withMockedReader(readerSpy);
      assertTrue(underTest.hasNext());
      MeasurementSample next = underTest.next();
      verify(readerSpy).readLine();
      assertEquals(sample, next);
      assertFalse(underTest.hasNext());
    } finally {
      if (underTest != null) {
        underTest.close();
      }
      sampleFile.delete();
    }
  }

  @Test
  void testReadsTenLines() throws IOException {
    String methodName = "testReadsTenLines";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);
    File sampleFile = Files.createTempFile(filePrefix, null /* suffix */).toFile();

    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(sampleFile)));
    BufferedReader readerSpy = spy(reader);

    List<MeasurementSample> samples = Lists.newArrayList();
    IntegerHashtag[] allTags = IntegerHashtag.values();
    for (int i = 0; i < 10; i++) {
      Set<IntegerHashtag> tags = Sets.newHashSet();
      for (int j = 0; j <= i; j++) {
        tags.add(allTags[j]);
      }
      MeasurementSample sample = new MeasurementSample(i /* timestamp */, i + 1 /* partitionNo */,
          UUID.randomUUID().toString(), tags);
      samples.add(sample);
    }

    // @formatter:off
    when(readerSpy.read())
      .thenReturn(1)
      .thenReturn(1)
      .thenReturn(1)
      .thenReturn(1)
      .thenReturn(1)
      .thenReturn(1)
      .thenReturn(1)
      .thenReturn(1)
      .thenReturn(1)
      .thenReturn(1)
      .thenReturn(-1);
    
    when(readerSpy.readLine())
      .thenReturn(samples.get(0).toString())
      .thenReturn(samples.get(1).toString())
      .thenReturn(samples.get(2).toString())
      .thenReturn(samples.get(3).toString())
      .thenReturn(samples.get(4).toString())
      .thenReturn(samples.get(5).toString())
      .thenReturn(samples.get(6).toString())
      .thenReturn(samples.get(7).toString())
      .thenReturn(samples.get(8).toString())
      .thenReturn(samples.get(9).toString());
    // @formatter:on

    MeasurementSampleReader underTest = MeasurementSampleReader.withMockedReader(readerSpy);
    try {
      for (int i = 1; i <= samples.size(); i++) {
        assertTrue(underTest.hasNext());
        MeasurementSample next = underTest.next();
        verify(readerSpy, times(i)).readLine();
        assertEquals(samples.get(i - 1), next);
        if (i == samples.size()) {
          assertFalse(underTest.hasNext());
        }
      }
    } finally {
      underTest.close();
      sampleFile.delete();
    }
  }


  @Test
  void testFailureWhenHasNextThrows() throws IOException {
    String methodName = "testFailureWhenHasNextThrows";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);
    File sampleFile = Files.createTempFile(filePrefix, null /* suffix */).toFile();

    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(sampleFile)));
    BufferedReader readerSpy = spy(reader);

    when(readerSpy.read()).thenThrow(new IOException());
    MeasurementSampleReader underTest = MeasurementSampleReader.withMockedReader(readerSpy);
    try {
      underTest.hasNext();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(UNEXPECTED_ERROR_WHILE_READING));
    } finally {
      underTest.close();
      sampleFile.delete();
    }
  }
  
  @Test
  void testFailureWhenNextThrows() throws IOException {
    String methodName = "testFailureWhenNextThrows";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);
    File sampleFile = Files.createTempFile(filePrefix, null /* suffix */).toFile();

    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(sampleFile)));
    BufferedReader readerSpy = spy(reader);

    when(readerSpy.readLine()).thenThrow(new IOException());
    MeasurementSampleReader underTest = MeasurementSampleReader.withMockedReader(readerSpy);
    try {
      underTest.next();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(UNEXPECTED_ERROR_WHILE_READING));
    } finally {
      underTest.close();
      sampleFile.delete();
    }
  }
}
