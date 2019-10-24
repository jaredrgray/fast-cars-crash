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

package com.tesla.interview.application.cli;

import static com.tesla.interview.application.cli.CommandLineInterviewApplication.executeWrapper;
import static com.tesla.interview.application.cli.CommandLineInterviewApplication.getOutputFiles;
import static com.tesla.interview.application.cli.CommandLineInterviewApplication.main;
import static org.apache.logging.log4j.LogManager.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.tesla.interview.application.InterviewApplication;
import com.tesla.interview.tests.InterviewTestCase;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;

public class TestCommandLineApplication extends InterviewTestCase {

  private static class FileDeletor implements FileVisitor<Path> {
    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
        throws IOException {
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      if (file.toFile().isFile()) {
        assertTrue(file.toFile().delete());
      }
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
      fail("visit failed");
      return FileVisitResult.TERMINATE;
    }
  }

  /**
   * This is a normal {@link CommandLineInterviewApplication} except that
   * {@link CommandLineInterviewApplication#appFactory} returns a mock.
   * <p/>
   * The idea is to prevent this test suite from calling {@link InterviewApplication}'s code, which
   * is outside the scope of a unit test.
   */
  private static class MockedCliApp extends CommandLineInterviewApplication {

    private MockedCliApp(CommandLineArgs args) {
      super(args);
    }

    private MockedCliApp(JCommander commander, CommandLineArgs args) {
      super(commander, args);

      appFactory = new AppFactory() {
        @Override
        public InterviewApplication get() {
          return mock(InterviewApplication.class);
        }
      };
    }

    private MockedCliApp(String[] args) {
      super(args);
    }
  }

  private static final Logger LOG = getLogger(TestCommandLineApplication.class);
  private static final String CSV_EXTENSION = ".csv";

  @Test
  void testConstructorWorksWithValidCliArgs(TestInfo testInfo) throws IOException {
    Path validDir = createTempDir(testInfo);
    Path validFile = createTempFile(testInfo);
    CommandLineArgs args = new CommandLineArgs();
    args.inputFile = validFile.toString();
    args.outputDirectory = validDir.toString();
    new MockedCliApp(args);
  }

  @Test
  void testExecuteWrapperDoesNotPrintUsageUponException(TestInfo testInfo) throws IOException {
    Path validDir = createTempDir(testInfo);
    Path invalidFile = createTempFile(testInfo);
    assertTrue(invalidFile.toFile().delete());

    CommandLineArgs args = new CommandLineArgs();
    args.inputFile = invalidFile.toString();
    args.outputDirectory = validDir.toString();
    args.isHelpCommand = false;
    args.numPartitions = 1;
    args.numWriteThreads = 1;

    JCommander spyCommander = spy(new JCommander());
    MockedCliApp app = new MockedCliApp(spyCommander, args);
    MockedCliApp appSpy = spy(app);
    doThrow(new NullPointerException()).when(appSpy).execute();

    try {
      executeWrapper(appSpy);
      fail("expected NullPointerException");
    } catch (NullPointerException e) {
      verify(spyCommander, never()).usage();
    }
  }

  @Test
  void testExecuteWrapperPrintsUsageUponInvalidArgument(TestInfo testInfo) throws IOException {
    Path validDir = createTempDir(testInfo);
    Path invalidFile = createTempFile(testInfo);
    assertTrue(invalidFile.toFile().delete());

    CommandLineArgs args = new CommandLineArgs();
    args.inputFile = invalidFile.toString();
    args.outputDirectory = validDir.toString();
    args.isHelpCommand = false;
    args.numPartitions = 1;
    args.numWriteThreads = 1;

    JCommander mockCommander = mock(JCommander.class, Mockito.RETURNS_DEEP_STUBS);
    MockedCliApp appSpy = spy(new MockedCliApp(mockCommander, args));
    doThrow(new ParameterException("POOP")).when(appSpy).parseArgs();

    executeWrapper(appSpy);
    verify(mockCommander).usage();
  }

  @Test
  void testGetOutputFilesSingle(TestInfo testInfo) throws IOException {
    Path tempDir = createTempDir(testInfo);
    List<String> filePaths = getOutputFiles(1 /* numPartitions */, tempDir);
    assertEquals(1, filePaths.size());
    assertTrue(filePaths.get(0).contains(testInfo.getTestMethod().get().getName()));
    assertTrue(filePaths.get(0).endsWith(1 + CSV_EXTENSION));
  }

  @Test
  void testGetTenOutputFiles(TestInfo testInfo) throws IOException {
    Path tempDir = createTempDir(testInfo);
    List<String> filePaths = getOutputFiles(10 /* numPartitions */, tempDir);
    assertEquals(10, filePaths.size());
    for (int i = 0; i < filePaths.size(); i++) {
      assertTrue(filePaths.get(i).contains(testInfo.getTestMethod().get().getName()));
      assertTrue(filePaths.get(i).endsWith((i + 1) + CSV_EXTENSION));
    }
  }

  @Test
  void testGetZeroOutputFiles(TestInfo testInfo) throws IOException {
    Path tempDir = createTempDir(testInfo);
    List<String> filePaths = getOutputFiles(0 /* numPartitions */, tempDir);
    assertEquals(0, filePaths.size());
  }

  @Test
  void testHelpCommandShowsUsage() {
    CommandLineArgs args = new CommandLineArgs();
    args.inputFile = "inputFile";
    args.isHelpCommand = true;
    args.numPartitions = 1;
    args.outputDirectory = "outdir";

    JCommander mockCmder = mock(JCommander.class);
    MockedCliApp underTest = new MockedCliApp(mockCmder, args);
    executeWrapper(underTest);
    verify(mockCmder).usage();
  }

  @Test
  void testMainExecutesSuccessfullyWithValidArguments(TestInfo testInfo) throws IOException {
    Path validDir = createTempDir(testInfo);
    Path validFile = createTempFile(testInfo);
    try {
      main(new String[] {"-i", validFile.toString(), "-o", validDir.toString(), "-p", "1", "-w",
          "1"});
    } finally {
      Files.walkFileTree(validDir, new FileDeletor());
    }
  }

  @Test
  void testMainWorksWithValidRawArgs(TestInfo testInfo) throws IOException {
    Path validDir = createTempDir(testInfo);
    Path validFile = createTempFile(testInfo);
    try {
      main(new String[] {"-i", validFile.toString(), "-o", validDir.toString(), "-p", "1", "-w",
          "1"});
    } finally {
      Files.walkFileTree(validDir, new FileDeletor());
    }
  }

  @Test
  void testValidateNullArgsFail(TestInfo testInfo) throws IOException {
    for (int i = 0; i + 1 < 1 << 2; i++) {
      Path validDir = null;
      Path validFile = null;
      try {
        validDir = createTempDir(testInfo);
        validFile = createTempFile(testInfo);

        // interpret i as a bitmask, where 0 indicates null
        CommandLineArgs args = new CommandLineArgs();
        args.inputFile = (i % 2) == 0 ? null : validFile.toString();
        args.outputDirectory = ((i >> 1) % 2) == 0 ? null : validDir.toString();

        // keep some code here to help a future programmer debug the bitmask
        LOG.info(String.format("%s: %02d -- inputFileNull: %s, outputDirectoryNull: %s",
            testInfo.getTestMethod().get().getName(), i, args.inputFile == null,
            args.outputDirectory == null));

        MockedCliApp underTest = new MockedCliApp(args);
        underTest.validateArgs();
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage().contains("cannot be null"));
      } finally {
        if (validDir != null) {
          assertTrue(validDir.toFile().delete());
        }
        if (validFile != null) {
          assertTrue(validFile.toFile().delete());
        }
      }
    }
  }

  @Test
  void testValidCommandDoesNotShowUsage() {
    CommandLineArgs args = new CommandLineArgs();
    args.inputFile = "inputFile";
    args.isHelpCommand = false;
    args.numPartitions = 1;
    args.outputDirectory = "outdir";

    JCommander mockCmder = mock(JCommander.class);
    MockedCliApp underTest = new MockedCliApp(mockCmder, args);
    underTest.execute();
    verify(mockCmder, never()).usage();
  }

}
