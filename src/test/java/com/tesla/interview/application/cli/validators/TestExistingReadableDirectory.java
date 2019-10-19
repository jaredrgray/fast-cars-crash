package com.tesla.interview.application.cli.validators;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.beust.jcommander.ParameterException;
import java.io.File;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

public class TestExistingReadableDirectory {

  private static final String MUST_BE_AN_EXISTING = "must be an existing";
  private static final String PARAM_NAME = "parameterName";

  @Test
  public void testNullValueFails() {
    ExistingReadableDirectory underTest = new ExistingReadableDirectory();
    try {
      underTest.validate(PARAM_NAME, null /* value */);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains("cannot be null"));
    }
  }

  @Test
  public void testEmptyValueFails() {
    ExistingReadableDirectory underTest = new ExistingReadableDirectory();
    try {
      underTest.validate(PARAM_NAME, "" /* value */);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains(MUST_BE_AN_EXISTING));
    }
  }

  @Test
  public void testNullPathFails() {
    ExistingReadableDirectory underTest = new ExistingReadableDirectory();
    try {
      underTest.validatePath(PARAM_NAME, null /* path */);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains(MUST_BE_AN_EXISTING));
    }
  }

  @Test
  public void testNullFileFails() {
    Path path = mock(Path.class);
    when(path.toFile()).thenReturn(null);
    ExistingReadableDirectory underTest = new ExistingReadableDirectory();
    try {
      underTest.validatePath(PARAM_NAME, path);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains(MUST_BE_AN_EXISTING));
    }
  }

  @Test
  public void testNoExistingFileFails() {
    ExistingReadableDirectory underTest = new ExistingReadableDirectory();
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
  public void testNotDirectoryFails() {
    Path path = mock(Path.class);
    File file = mock(File.class);
    when(path.toFile()).thenReturn(file);
    when(file.exists()).thenReturn(true);
    when(file.isDirectory()).thenReturn(false);

    ExistingReadableDirectory underTest = new ExistingReadableDirectory();
    try {
      underTest.validatePath(PARAM_NAME, path);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains(MUST_BE_AN_EXISTING));
    }
  }

  @Test
  public void testNotReadableFails() {
    ExistingReadableDirectory underTest = new ExistingReadableDirectory();
    Path path = mock(Path.class);
    File file = mock(File.class);
    when(path.toFile()).thenReturn(file);
    when(file.exists()).thenReturn(true);
    when(file.isDirectory()).thenReturn(true);
    when(file.canRead()).thenReturn(false);

    try {
      underTest.validatePath(PARAM_NAME, path);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains(MUST_BE_AN_EXISTING));
    }
  }

  @Test
  public void testReadableDirectorySucceeds() {
    Path path = mock(Path.class);
    File file = mock(File.class);
    when(path.toFile()).thenReturn(file);
    when(file.exists()).thenReturn(true);
    when(file.isDirectory()).thenReturn(true);
    when(file.canRead()).thenReturn(true);
    
    ExistingReadableDirectory underTest = new ExistingReadableDirectory();
    underTest.validatePath(PARAM_NAME, path);
  }

}
