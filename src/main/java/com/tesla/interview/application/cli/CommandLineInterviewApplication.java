package com.tesla.interview.application.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.internal.Console;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.tesla.interview.application.InterviewApplication;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class CommandLineInterviewApplication {

  protected class AppFactory implements Supplier<InterviewApplication> {
    @Override
    public InterviewApplication get() {
      Path outputDirectory = Paths.get(parsedArguments.outputDirectory);
      List<String> outputFilePaths = getOutputFiles(parsedArguments.numPartitions, outputDirectory);
      return new InterviewApplication(parsedArguments.numWriteThreads,
          Integer.MAX_VALUE /* TODO: maxFileHandles */, outputFilePaths, parsedArguments.inputFile);
    }
  }

  private static final String OUTPUT_FILE_FORMAT = "output-file-%d.csv";

  /**
   * Build a list of output files.
   * <p>
   * Package-visible for unit tests.
   * </p>
   * 
   * @param numPartitions number of partitions
   * @param outputDirectory non-null path to output directory
   * @return list of output files within the output directory
   */
  static List<String> getOutputFiles(int numPartitions, Path outputDirectory) {
    List<String> outputFilePaths = Lists.newArrayList();
    for (int i = 1; i <= numPartitions; i++) {
      String outputFileName = String.format(OUTPUT_FILE_FORMAT, i);
      Path outputFilePath = outputDirectory.resolve(outputFileName);
      outputFilePaths.add(outputFilePath.toString());
    }
    return outputFilePaths;
  }

  /**
   * Run the application from the command line.
   * 
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    CommandLineInterviewApplication cliApp = new CommandLineInterviewApplication(args);
    try {
      InterviewApplication app = cliApp.toInterviewApplication();
      app.call();
    } catch (ParameterException e) {
      cliApp.commander.usage();
      cliApp.commander.getConsole().println(String.format("ERROR: %s", e.getMessage()));
    } catch (RuntimeException e) {
      cliApp.commander.usage();
      cliApp.commander.getConsole().println(String.format("ERROR: %s", e.getMessage()));
      printTrace(cliApp.commander.getConsole(), e);
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

  protected AppFactory appFactory = new AppFactory();

  private final JCommander commander;

  private CommandLineArgs parsedArguments;

  CommandLineInterviewApplication(CommandLineArgs args) {
    this(new JCommander(args), args);
  }

  CommandLineInterviewApplication(JCommander commander, CommandLineArgs args) {
    this.commander = commander;
    validateArgs(args);
    this.parsedArguments = args;

  }

  /**
   * Constructor.
   * <p>
   * Package-visible for unit tests.
   * </p>
   * 
   * @param args command-line arguments
   */
  public CommandLineInterviewApplication(String[] args) {
    this.commander = new JCommander();
    CommandLineArgs parsedArguments = new CommandLineArgs();
    commander.parse(args);
    validateArgs(parsedArguments);
    this.parsedArguments = parsedArguments;
  }

  /**
   * Convert our parsed commands to an application.
   * <p>
   * Package-visible for unit tests.
   * </p>
   * 
   * @return the application
   */
  InterviewApplication toInterviewApplication() {
    if (!parsedArguments.isHelpCommand) {
      return appFactory.get();
    } else {
      // just displaying help -- don't give back an apps
      commander.usage();
      return null;
    }
  }

  /**
   * Validate the arguments.
   * 
   * @param parsedArguments arguments to validate
   * @throws ParameterException if arguments are invalid
   */
  private void validateArgs(CommandLineArgs parsedArguments) {
    if (parsedArguments.outputDirectory == null) {
      throw new ParameterException("outputDirectory cannot be null");
    }
    if (parsedArguments.inputFile == null) {
      throw new ParameterException("inputFile cannot be null");
    }
  }

}
