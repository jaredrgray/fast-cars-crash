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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCommandLineApplication {

  /**
   * This is a normal {@link CommandLineInterviewApplication} except that
   * {@link CommandLineInterviewApplication#appFactory} returns a mock.
   * <p/>
   * The idea is to prevent this test suite from calling {@link InterviewApplication}'s code, which
   * is outside the scope of a unit test.
   */
  private class MockedCliApp extends CommandLineInterviewApplication {

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
  void testConstructorWorksWithValidCliArgs() throws IOException {
    String methodName = "testConstructorWorksWithValidCliArgs";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);

    Path validDir = Files.createTempDirectory(filePrefix);;
    Path validFile = Files.createTempFile(filePrefix, null /* suffix */);;

    CommandLineArgs args = new CommandLineArgs();
    args.inputFile = validFile.toString();
    args.outputDirectory = validDir.toString();
    try {
      new MockedCliApp(args);
    } finally {
      validDir.toFile().delete();
      validFile.toFile().delete();
    }
  }

  @Test
  void testExecuteWrapperDoesNotPrintUsageUponException() throws IOException {
    String methodName = "testExecuteWrapperDoesNotPrintUsageUponException";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);

    Path validDir = Files.createTempDirectory(filePrefix);;
    Path invalidFile = Files.createTempFile(filePrefix, null /* suffix */);
    invalidFile.toFile().delete();

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
    } finally {
      validDir.toFile().delete();
    }
  }

  @Test
  void testExecuteWrapperPrintsUsageUponInvalidArgument() throws IOException {
    String methodName = "testExecuteWrapperPrintsUsageUponInvalidArgument";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);

    Path validDir = Files.createTempDirectory(filePrefix);;
    Path invalidFile = Files.createTempFile(filePrefix, null /* suffix */);
    invalidFile.toFile().delete();

    CommandLineArgs args = new CommandLineArgs();
    args.inputFile = invalidFile.toString();
    args.outputDirectory = validDir.toString();
    args.isHelpCommand = false;
    args.numPartitions = 1;
    args.numWriteThreads = 1;

    JCommander mockCommander = mock(JCommander.class, Mockito.RETURNS_DEEP_STUBS);
    MockedCliApp appSpy = spy(new MockedCliApp(mockCommander, args));
    doThrow(new ParameterException("POOP")).when(appSpy).parseArgs();

    try {
      executeWrapper(appSpy);
      verify(mockCommander).usage();
    } finally {
      validDir.toFile().delete();
    }
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
  void testHelpCommandShowsUsage() {
    CommandLineArgs args = new CommandLineArgs();
    args.inputFile = "inputFile";
    args.isHelpCommand = true;
    args.numPartitions = 1;
    args.outputDirectory = "outdir";

    JCommander mockCmder = mock(JCommander.class);
    MockedCliApp underTest = new MockedCliApp(mockCmder, args);
    underTest.execute();
    verify(mockCmder).usage();
  }

  @Test
  void testMainExecutesSuccessfullyWithValidArguments() throws IOException {
    String methodName = "testMainExecutesSuccessfullyWithValidArguments";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);

    Path validDir = Files.createTempDirectory(filePrefix);;
    Path validFile = Files.createTempFile(filePrefix, null /* suffix */);
    String[] args =
        new String[] {"-i", validFile.toString(), "-o", validDir.toString(), "-p", "1", "-w", "1"};

    try {
      main(args);
    } finally {
      validDir.toFile().delete();
      validFile.toFile().delete();
    }
  }

  @Test
  void testMainWorksWithValidRawArgs() throws IOException {
    String methodName = "testConstructorWorksWithValidRawArgs";
    String filePrefix = String.format("%s_%s", getClass().getName(), methodName);

    Path validDir = Files.createTempDirectory(filePrefix);;
    Path validFile = Files.createTempFile(filePrefix, null /* suffix */);

    try {
      main(new String[] {"-i", validFile.toString(), "-o", validDir.toString(), "-p", "1", "-w",
          "1"});
    } finally {
      validDir.toFile().delete();
      validFile.toFile().delete();
    }
  }

  @Test
  void testValidateNullArgsFail() throws IOException {
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
        LOG.info(String.format("%s: %02d -- inputFileNull: %s, outputDirectoryNull: %s", methodName,
            i, args.inputFile == null, args.outputDirectory == null));

        MockedCliApp underTest = new MockedCliApp(args);
        underTest.validateArgs();
        fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
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
    MockedCliApp underTest = new MockedCliApp(mockCmder, args);
    underTest.execute();
    verify(mockCmder, never()).usage();
  }

}
