package com.tesla.interview.application.cli.validators;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.beust.jcommander.ParameterException;
import java.io.File;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

public class TestExistingReadableFile {

  private static final String MUST_BE_AN_EXISTING = "must be an existing";
  private static final String PARAM_NAME = "parameterName";

  @Test
  public void testNullValueFails() {
    ExistingReadableFile underTest = new ExistingReadableFile();
    try {
      underTest.validate(PARAM_NAME, null /* value */);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains("cannot be null"));
    }
  }

  @Test
  public void testEmptyValueFails() {
    ExistingReadableFile underTest = new ExistingReadableFile();
    try {
      underTest.validate(PARAM_NAME, "" /* value */);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains(MUST_BE_AN_EXISTING));
    }
  }

  @Test
  public void testNullPathFails() {
    ExistingReadableFile underTest = new ExistingReadableFile();
    try {
      underTest.validatePath(PARAM_NAME, null /* path */);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains(MUST_BE_AN_EXISTING));
    }
  }

  @Test
  public void testNullFileFails() {
    ExistingReadableFile underTest = new ExistingReadableFile();
    Path path = mock(Path.class);
    when(path.toFile()).thenReturn(null);
    try {
      underTest.validatePath(PARAM_NAME, path);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains(MUST_BE_AN_EXISTING));
    }
  }

  @Test
  public void testNotExistingFails() {
    ExistingReadableFile underTest = new ExistingReadableFile();
    Path path = mock(Path.class);
    File file = mock(File.class);
    when(path.toFile()).thenReturn(file);
    when(file.exists()).thenReturn(false);

    try {
      underTest.validatePath(PARAM_NAME, path);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains(MUST_BE_AN_EXISTING));
    }
  }

  @Test
  public void testNonFileFails() {
    ExistingReadableFile underTest = new ExistingReadableFile();
    Path path = mock(Path.class);
    File file = mock(File.class);
    when(path.toFile()).thenReturn(file);
    when(file.exists()).thenReturn(true);
    when(file.isFile()).thenReturn(false);

    try {
      underTest.validatePath(PARAM_NAME, path);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains(MUST_BE_AN_EXISTING));
    }
  }

  @Test
  public void testNonReadableFails() {
    Path path = mock(Path.class);
    File file = mock(File.class);
    when(path.toFile()).thenReturn(file);
    when(file.exists()).thenReturn(true);
    when(file.isFile()).thenReturn(true);
    when(file.canRead()).thenReturn(false);

    ExistingReadableFile underTest = new ExistingReadableFile();
    try {
      underTest.validatePath(PARAM_NAME, path);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains(MUST_BE_AN_EXISTING));
    }
  }

  @Test
  public void testReadableSucceeds() {
    Path path = mock(Path.class);
    File file = mock(File.class);
    when(path.toFile()).thenReturn(file);
    when(file.exists()).thenReturn(true);
    when(file.isFile()).thenReturn(true);
    when(file.canRead()).thenReturn(true);

    ExistingReadableFile underTest = new ExistingReadableFile();
    underTest.validatePath(PARAM_NAME, path);
  }

}
