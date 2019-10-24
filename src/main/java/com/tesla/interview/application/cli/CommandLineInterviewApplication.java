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

import static com.tesla.interview.application.ApplicationTools.consoleTrace;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.tesla.interview.application.InterviewApplication;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

/**
 * Command line wrapper for {@link InterviewApplication}.
 */
public class CommandLineInterviewApplication {

  /**
   * Produces an {@link InterviewApplication} corresponding to the CLI input parameters. Can be
   * subclassed in to deliver sufficiently loose coupling for unit tests.
   */
  protected class AppFactory implements Supplier<InterviewApplication> {
    @Override
    public InterviewApplication get() {
      Path outputDirectory = Paths.get(parsedArguments.outputDirectory);
      List<String> outputFilePaths = getOutputFiles(parsedArguments.numPartitions, outputDirectory);
      return new InterviewApplication(parsedArguments.numWriteThreads,
          Integer.MAX_VALUE /* TODO: maxFileHandles */, outputFilePaths, parsedArguments.inputFile,
          queueSize, DEFAULT_POLL_DURATION);
    }
  }

  private static final String OUTPUT_FILE_FORMAT = "output-file-%d.csv";
  private static final int DEFAULT_QUEUE_SIZE = 100;
  private static final Duration DEFAULT_POLL_DURATION = Duration.ofSeconds(1);

  /**
   * Run the application from the command line.
   * 
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    executeWrapper(new CommandLineInterviewApplication(args, DEFAULT_QUEUE_SIZE));
  }

  /**
   * Execute the specified application after parsing and validating its arguments. Print input
   * validation exceptions to the console in a single line below the usage. Print a stack trace if
   * an unexpected exception occurs.
   * <p/>
   * Package-visible for unit tests.
   * 
   * @param cliApp application to execute
   */
  static void executeWrapper(CommandLineInterviewApplication cliApp) {
    try {
      cliApp.parseArgs();
      cliApp.validateArgs();
      if (!cliApp.parsedArguments.isHelpCommand) {
        // validation successful; execute app
        cliApp.execute();
      } else {
        // just printing help
        cliApp.commander.usage();
      }
    } catch (RuntimeException e) {
      if (e instanceof ParameterException || e instanceof IllegalArgumentException) {
        // validation failed; print usage and exception to console
        cliApp.commander.usage();
        cliApp.commander.getConsole().println(e.getMessage());
      } else {
        // unexpected exception; dump stack trace
        cliApp.commander.getConsole()
            .println(String.format("unexpected error: %s", e.getMessage()));
        consoleTrace(cliApp.commander.getConsole(), e);
        throw e;
      }
    }

  }

  /**
   * Build a list of paths to output files.
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
  CommandLineArgs parsedArguments;

  final int queueSize;
  final JCommander commander;
  final String[] rawArgs;

  /**
   * Primary constructor. Our main method calls this.
   * 
   * @param args command line arguments
   * @param queueSize max. number of in-flight write requests
   */
  public CommandLineInterviewApplication(String[] args, int queueSize) {
    this(new JCommander(), args, queueSize);
  }

  /**
   * Injection constructor for unit tests.
   * 
   * @param args command line arguments to inject
   * @param queueSize queue size to inject
   */
  CommandLineInterviewApplication(CommandLineArgs args, int queueSize) {
    this(new JCommander(args), args, queueSize);
  }

  /**
   * Injection constructor for unit tests.
   * 
   * @param commander commander to inject
   * @param args parsed args to inject
   * @param queueSize queue size to inject
   */
  CommandLineInterviewApplication(JCommander commander, CommandLineArgs args, int queueSize) {
    this.commander = commander;
    this.parsedArguments = args;
    this.rawArgs = null;
    this.queueSize = queueSize;
  }

  /**
   * Injection constructor for unit tests.
   * 
   * @param commander commander to inject
   * @param args command line arguments to inject
   * @param queueSize queue size to inject
   */
  CommandLineInterviewApplication(JCommander commander, String[] args, int queueSize) {
    this.commander = commander;
    this.parsedArguments = null;
    this.rawArgs = args;
    this.queueSize = queueSize;
  }

  /**
   * Execute this application.
   */
  void execute() {
    if (parsedArguments != null) {
      InterviewApplication app = appFactory.get();
      if (app != null) {
        // not-null check is needed for unit tests only, where a dummy factory is provided
        app.call();
      }
    } else {
      throw new IllegalStateException("Caller did not validate arguments");
    }
  }

  /**
   * Parse the input args.
   * 
   * @throws IllegalArgumentException when there is a problem with one or more arguments provided
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
