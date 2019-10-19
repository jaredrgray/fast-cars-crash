package com.tesla.interview.application;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

public class TestAsynchronousWriter {

  private static final String MUST_BE_POSITIVE = "must be positive";
  private List<Path> filesToWrite;
  Map<Integer, String> partitionToPath;
  AsynchronousWriter underTest;

  @BeforeEach
  @Order(1)
  void beforeEach() throws IOException {
    filesToWrite = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      File createdFile = File.createTempFile(getClass().getName(), null /* suffix */);
      createdFile.delete();
      filesToWrite.add(Paths.get(createdFile.getPath()));
    }

    partitionToPath = Maps.newHashMap();
    int fileNo = 0;
    while (fileNo < filesToWrite.size()) {
      String nextPath = filesToWrite.get(fileNo).toString();
      partitionToPath.put(fileNo, nextPath);
      fileNo++;
    }
  }

  @AfterEach
  void afterEach() {
    if (underTest != null) {
      underTest.close();
    }

    for (int i = 0; i < filesToWrite.size(); i++) {
      File createdFile = filesToWrite.get(i).toFile();
      if (createdFile.exists()) {
        createdFile.delete();
      }
    }
  }

  @Test
  void testConstructorPositive() {
    underTest = new AsynchronousWriter(1 /* threadPoolSize */, partitionToPath);
  }

  @Test
  void testConstructorWithNegativePoolSizeFails() {
    try {
      underTest = new AsynchronousWriter(-1 /* threadPoolSize */, partitionToPath);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(MUST_BE_POSITIVE));
    }
  }

  @Test
  void testConstructorWithEmptyPoolFails() {
    try {
      underTest = new AsynchronousWriter(0 /* threadPoolSize */, partitionToPath);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(MUST_BE_POSITIVE));
    }
  }

  @Test
  void testConstructorWithDuplicatePathsFails() {
    Map<Integer, String> customMap = Maps.newHashMap();
    customMap.putAll(partitionToPath);

    String dupPath = customMap.get(1);
    customMap.put(2, dupPath);

    try {
      underTest = new AsynchronousWriter(1 /* threadPoolSize */, customMap);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot specify identical path"));
    }
  }

  @Test
  void testConstructorWithMissingFileFails() throws IOException {
    Map<Integer, String> customMap = Maps.newHashMap();
    customMap.putAll(partitionToPath);

    Path tempDir = Files.createTempDirectory(null /* prefix */);
    tempDir.toFile().delete();
    String pathWithNoFile = tempDir.resolve("iDoNotExist").toString();
    customMap.put(1, pathWithNoFile);

    try {
      underTest = new AsynchronousWriter(1 /* threadPoolSize */, customMap);
      fail("expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Unable to open"));
    }
  }

  @Test
  void testCloseHappyPath() {
    underTest = new AsynchronousWriter(1 /* threadPoolSize */, partitionToPath);
    underTest.close();
  }
}
