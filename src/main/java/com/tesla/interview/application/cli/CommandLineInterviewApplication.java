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

  static void executeWrapper(CommandLineInterviewApplication cliApp) {
    try {
      cliApp.execute();
    } catch (RuntimeException e) {
      cliApp.commander.usage();
      cliApp.commander.getConsole().println(String.format("ERROR: %s", e.getMessage()));
      printTrace(cliApp.commander.getConsole(), e);
    }
  }

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
    executeWrapper(new CommandLineInterviewApplication(args));
  }

  /**
   * Print a stack trace of the input exception to the provided console.
   * <p>
   * Package-visible for unit tests.
   * </p>
   * 
   * @param console console to which to print
   * @param th exception to trace
   */
  static void printTrace(Console console, Throwable th) {

    // get outermost exception
    if (th.getCause() == null) {
      console.println("Stack trace:");
      StackTraceElement[] elements = th.getStackTrace();
      int numDigits = String.valueOf(elements.length).length();

      // print to console
      String depthFormat = "%" + "0" + numDigits + "d";
      for (int i = 1; i <= elements.length; i++) {
        String depth = String.format(depthFormat, i);
        // @formatter:off
        console.println(new StringBuilder()
            .append(depth)
            .append(": ")
            .append(elements[i - 1].toString())
            .toString());
        // @formatter:on
      }
    } else {
      // recurse
      printTrace(console, th.getCause());
    }
  }

  protected AppFactory appFactory = new AppFactory();

  private final JCommander commander;
  private final CommandLineArgs parsedArguments;

  CommandLineInterviewApplication(CommandLineArgs args) {
    this(new JCommander(args), args);
  }

  CommandLineInterviewApplication(JCommander commander, CommandLineArgs args) {
    this.commander = commander;
    this.parsedArguments = args;
    validateArgs();
  }

  CommandLineInterviewApplication(JCommander commander, String[] args) {
    this.commander = commander;
    this.parsedArguments = new CommandLineArgs();
    commander.addObject(parsedArguments);
    commander.parse(args);
    validateArgs();
  }

  /**
   * Primary constructor.
   * 
   * @param args command-line arguments
   */
  public CommandLineInterviewApplication(String[] args) {
    this(new JCommander(), args);
  }

  /**
   * Execute this application.
   */
  void execute() {
    if (!parsedArguments.isHelpCommand) {
      InterviewApplication app = appFactory.get();
      if (app != null) {
        app.call();
      }
    } else {
      // just displaying help -- don't give back an apps
      commander.usage();
    }
  }

  /**
   * Validate the arguments.
   * 
   * @param parsedArguments arguments to validate
   * @throws ParameterException if arguments are invalid
   */
  void validateArgs() {
    if (parsedArguments.outputDirectory == null) {
      throw new ParameterException("outputDirectory cannot be null");
    }
    if (parsedArguments.inputFile == null) {
      throw new ParameterException("inputFile cannot be null");
    }
  }

}
