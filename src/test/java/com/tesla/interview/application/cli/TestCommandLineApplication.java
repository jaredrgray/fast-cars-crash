package com.tesla.interview.application.cli;

import static com.tesla.interview.application.cli.CommandLineInterviewApplication.getOutputFiles;
import static com.tesla.interview.application.cli.CommandLineInterviewApplication.printTrace;
import static org.apache.logging.log4j.LogManager.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.internal.Console;
import com.tesla.interview.application.InterviewApplication;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

public class TestCommandLineApplication {

  private static final Logger LOG = getLogger(TestCommandLineApplication.class);

  private static final String CSV_EXTENSION = ".csv";

  class DummyCliApp extends CommandLineInterviewApplication {
    public DummyCliApp(JCommander commander, CommandLineArgs args) {
      super(commander, args);
      appFactory = new AppFactory() {
        @Override
        public InterviewApplication get() {
          return null;
        }
      };
    }
  }

  @Test
  void testConstructorWorksWithValidRawArgs() throws IOException {
    String methodName = "testConstructorNullArgsFail";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);

    Path validDir = Files.createTempDirectory(filePrefix);;
    Path validFile = Files.createTempFile(filePrefix, null /* suffix */);;

    String[] args =
        new String[] {"-i", validFile.toString(), "-o", validDir.toString(), "-p", "1", "-w", "1"};
    try {
      new CommandLineInterviewApplication(args);
    } finally {
      validDir.toFile().delete();
      validFile.toFile().delete();
    }
  }

  @Test
  void testConstructorWorksWithValidCliArgs() throws IOException {
    String methodName = "testConstructorNullArgsFail";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);

    Path validDir = Files.createTempDirectory(filePrefix);;
    Path validFile = Files.createTempFile(filePrefix, null /* suffix */);;

    CommandLineArgs args = new CommandLineArgs();
    args.inputFile = validFile.toString();
    args.outputDirectory = validDir.toString();
    try {
      new CommandLineInterviewApplication(args);
    } finally {
      validDir.toFile().delete();
      validFile.toFile().delete();
    }
  }

  @Test
  void testConstructorEmptyArgsFail() {
    String[] args = new String[] {};
    try {
      new CommandLineInterviewApplication(args);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains("cannot be null"));
    }
  }

  @Test
  void testConstructorNullArgsFail() throws IOException {
    String methodName = "testConstructorNullArgsFail";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);

    for (int i = 0; i + 1 < 1 << 2; i++) {
      Path validDir = null;
      Path validFile = null;
      try {
        validDir = Files.createTempDirectory(filePrefix);
        validFile = Files.createTempFile(filePrefix, null /* suffix */);

        // interpret i as a bitmask, where 0 indicates null
        CommandLineArgs args = new CommandLineArgs();
        args.inputFile = (i % 2) == 0 ? null : validFile.toString();
        args.outputDirectory = ((i >> 1) % 2) == 0 ? null : validDir.toString();

        // keep some code here to help a future programmer debug the bitmask
        LOG.info(String.format("%s: %02d -- inputFileNull: %s, outputDirectoryNull: %s%n",
            methodName, i, args.inputFile == null, args.outputDirectory == null));

        new CommandLineInterviewApplication(args);
        fail("Expected ParameterException");
      } catch (ParameterException e) {
        assertTrue(e.getMessage().contains("cannot be null"));
      } finally {
        if (validDir != null) {
          validDir.toFile().delete();
        }
        if (validFile != null) {
          validFile.toFile().delete();
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
    CommandLineInterviewApplication underTest = new DummyCliApp(mockCmder, args);
    underTest.toInterviewApplication();
    verify(mockCmder, never()).usage();
  }

  @Test
  void testHelpCommandShowsUsage() {
    CommandLineArgs args = new CommandLineArgs();
    args.inputFile = "inputFile";
    args.isHelpCommand = true;
    args.numPartitions = 1;
    args.outputDirectory = "outdir";

    JCommander mockCmder = mock(JCommander.class);
    CommandLineInterviewApplication underTest =
        new CommandLineInterviewApplication(mockCmder, args);
    InterviewApplication app = underTest.toInterviewApplication();
    assertNull(app);
    verify(mockCmder).usage();
  }

  @Test
  void testGetOutputFilesSingle() throws IOException {
    String methodName = "testGetOutputFilesSingle";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);
    Path tempDir = Files.createTempDirectory(filePrefix);
    try {
      List<String> filePaths = getOutputFiles(1 /* numPartitions */, tempDir);
      assertEquals(1, filePaths.size());
      assertTrue(filePaths.get(0).contains(filePrefix));
      assertTrue(filePaths.get(0).endsWith(1 + CSV_EXTENSION));
    } finally {
      tempDir.toFile().delete();
    }
  }

  @Test
  void testGetTenOutputFiles() throws IOException {
    String methodName = "testGetTenOutputFiles";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);
    Path tempDir = Files.createTempDirectory(filePrefix);
    try {
      List<String> filePaths = getOutputFiles(10 /* numPartitions */, tempDir);
      assertEquals(10, filePaths.size());
      for (int i = 0; i < filePaths.size(); i++) {
        assertTrue(filePaths.get(i).contains(filePrefix));
        assertTrue(filePaths.get(i).endsWith((i + 1) + CSV_EXTENSION));
      }
    } finally {
      tempDir.toFile().delete();
    }
  }

  @Test
  void testGetZeroOutputFiles() throws IOException {
    String methodName = "testGetZeroOutputFiles";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);
    Path tempDir = Files.createTempDirectory(filePrefix);
    try {
      List<String> filePaths = getOutputFiles(0 /* numPartitions */, tempDir);
      assertEquals(0, filePaths.size());
    } finally {
      tempDir.toFile().delete();
    }
  }

  @Test
  void testPrintTraceSimple() {
    Console mockConsole = mock(Console.class);
    Exception onion = new Exception();
    printTrace(mockConsole, onion);
    verify(mockConsole, times(onion.getStackTrace().length + 1)).println(anyString());
  }

  @Test
  void testPrintTraceWithNested() {
    Console mockConsole = mock(Console.class);
    Exception onion = new Exception(new Exception(new Exception()));
    printTrace(mockConsole, onion);
    verify(mockConsole, times(onion.getStackTrace().length + 1)).println(anyString());
  }

}
