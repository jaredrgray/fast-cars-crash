package com.tesla.interview.application.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.internal.Console;
import com.google.common.collect.Lists;
import com.tesla.interview.application.InterviewApplication;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class CommandLineInterviewApplication {

  private static final String OUTPUT_FILE_FORMAT = "output-file-%d.csv";

  /**
   * Run the application from the command line.
   * 
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    CommandLineInterviewApplication app = new CommandLineInterviewApplication(args);
    try {
      app.commander.parse(args);
      app.execute();
    } catch (ParameterException e) {
      app.commander.usage();
      app.commander.getConsole().println(String.format("ERROR: %s", e.getMessage()));
    } catch (RuntimeException e) {
      app.commander.usage();
      app.commander.getConsole().println(String.format("ERROR: %s", e.getMessage()));
      printTrace(app.commander.getConsole(), e);
    }
  }

  private static void printTrace(Console console, Throwable th) {
    if (th.getCause() == null) {
      // print trace of outermost exception
      console.println("Stack trace:");
      StackTraceElement[] elements = th.getStackTrace();
      int numDigits = String.valueOf(elements.length).length();
      String depthFormat = "%" + "0" + numDigits + "d";
      for (int i = 1; i <= elements.length; i++) {
        String depth = String.format(depthFormat, i);
        console.println(depth + ": " + elements[i - 1].toString());
      }
    } else {
      // recurse
      printTrace(console, th.getCause());
    }
  }

  private final JCommander commander;
  private final CommandLineArgs parsedArguments;

  /**
   * Constructor.
   * 
   * @param args command-line arguments
   */
  private CommandLineInterviewApplication(String[] args) {
    parsedArguments = new CommandLineArgs();
    commander = new JCommander(parsedArguments);
  }

  private void execute() {
    if (!parsedArguments.isHelpCommand) {
      // build output file list
      Path outputDirectory = Paths.get(parsedArguments.outputDirectory);
      List<String> outputFilePaths = Lists.newArrayList();
      for (int i = 1; i <= parsedArguments.numPartitions; i++) {
        String outputFileName = String.format(OUTPUT_FILE_FORMAT, i);
        Path outputFilePath = outputDirectory.resolve(outputFileName);
        outputFilePaths.add(outputFilePath.toString());
      }

      // construct app
      InterviewApplication app = new InterviewApplication(parsedArguments.numWriteThreads,
          Integer.MAX_VALUE /* TODO: maxFileHandles */, outputFilePaths, parsedArguments.inputFile);
      app.call();
    } else {
      commander.usage();
    }
  }
}
