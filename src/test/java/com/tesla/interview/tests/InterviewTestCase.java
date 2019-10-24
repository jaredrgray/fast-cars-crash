package com.tesla.interview.tests;

import static org.apache.logging.log4j.LogManager.getLogger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Stack;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

public abstract class InterviewTestCase {

  private static final Logger LOG = getLogger(InterviewTestCase.class);

  private Stack<Path> createdFiles;

  @AfterEach
  protected void cleanUp() {
    while (!createdFiles.isEmpty()) {
      Path next = createdFiles.pop();
      if (next != null && next.toFile() != null && next.toFile().exists()) {
        if (!next.toFile().delete()) {
          LOG.warn(String.format("Unable to delete file -- path: %d", next));
        }
      }
    }
  }

  protected Path createTempDir(TestInfo testInfo) throws IOException {
    String className = testInfo.getClass().getSimpleName();
    String methodName = testInfo.getTestMethod().get().getName();
    String prefix = String.format("%s_%s_", className, methodName);
    Path created = Files.createTempDirectory(prefix);
    createdFiles.push(created);
    return created;
  }

  protected Path createTempFile(TestInfo testInfo) throws IOException {
    String className = testInfo.getTestClass().get().getSimpleName();
    String methodName = testInfo.getTestMethod().get().getName();
    String prefix = String.format("%s_%s_", className, methodName);
    Path created = Files.createTempFile(prefix, null /* suffix */);
    createdFiles.push(created);
    return created;
  }

  @BeforeEach
  protected void initialize() {
    createdFiles = new Stack<>();
  }
}
