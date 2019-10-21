package com.tesla.interview.application.cli;

import static com.tesla.interview.application.ApplicationTools.consoleTrace;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
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
   * Run the application from the command line.
   * 
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    executeWrapper(new CommandLineInterviewApplication(args));
  }

  static void executeWrapper(CommandLineInterviewApplication cliApp) {
    try {
      cliApp.parseArgs();
      cliApp.validateArgs();
      if (!cliApp.parsedArguments.isHelpCommand) {
        cliApp.execute();
      } else {
        // just printing help
        cliApp.commander.usage();
      }
    } catch (RuntimeException e) {
      if (e instanceof ParameterException || e instanceof IllegalArgumentException) {
        // validation failed
        consoleTrace(cliApp.commander.getConsole(), e);
        cliApp.commander.usage();
        cliApp.commander.getConsole()
            .println(String.format("invalid parameter: %s", e.getMessage()));
      } else {
        // unexpected exception: dump stack trace
        cliApp.commander.getConsole()
            .println(String.format("unexpected error: %s", e.getMessage()));
        consoleTrace(cliApp.commander.getConsole(), e);
        throw e;
      }
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

  AppFactory appFactory = new AppFactory();
  final JCommander commander;
  final String[] rawArgs;
  CommandLineArgs parsedArguments;

  /**
   * Primary constructor. Our main method calls this.
   * 
   * @param args command-line arguments
   */
  public CommandLineInterviewApplication(String[] args) {
    this(new JCommander(), args);
  }

  CommandLineInterviewApplication(CommandLineArgs args) {
    this(new JCommander(args), args);
  }

  /**
   * Injection constructor for unit tests.
   * 
   * @param commander commander to inject
   * @param args parsed args to inject
   */
  CommandLineInterviewApplication(JCommander commander, CommandLineArgs args) {
    this.commander = commander;
    this.parsedArguments = args;
    this.rawArgs = null;
  }

  CommandLineInterviewApplication(JCommander commander, String[] args) {
    this.commander = commander;
    this.parsedArguments = null;
    this.rawArgs = args;
  }

  /**
   * Execute this application.
   */
  void execute() {
    if (parsedArguments != null) {
      InterviewApplication app = appFactory.get();
      if (app != null) {
        app.call();
      }
    } else {
      throw new IllegalStateException("Caller did not validate arguments");
    }
  }

  /**
   * Parse the input args.
   * 
   * @throws ParameterException when there is a problem with one or more arguments provided
   */
  void parseArgs() throws ParameterException {
    if (parsedArguments == null) {
      parsedArguments = new CommandLineArgs();
      commander.addObject(parsedArguments);
      commander.parse(rawArgs);
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
      throw new IllegalArgumentException("outputDirectory cannot be null");
    }
    if (parsedArguments.inputFile == null) {
      throw new IllegalArgumentException("inputFile cannot be null");
    }
  }

}
