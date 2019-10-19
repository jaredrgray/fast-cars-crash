package com.tesla.interview.application.cli;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.tesla.interview.application.InterviewApplication;
import org.junit.jupiter.api.Test;

public class TestCommandLineApplication {

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
  };

  @Test
  void testConstructEmptyArgsFail() {
    String[] args = new String[] {};
    try {
      new CommandLineInterviewApplication(args);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains("cannot be null"));
    }
  }

  @Test
  void testNonHelpCommand() {
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
  void testHelpCommand() {
    CommandLineArgs args = new CommandLineArgs();
    args.inputFile = "inputFile";
    args.isHelpCommand = true;
    args.numPartitions = 1;
    args.outputDirectory = "outdir";

    JCommander mockCmder = mock(JCommander.class);
    CommandLineInterviewApplication underTest = new CommandLineInterviewApplication(mockCmder, args);
    InterviewApplication app = underTest.toInterviewApplication();
    assertNull(app);
    verify(mockCmder).usage();
  }
}
