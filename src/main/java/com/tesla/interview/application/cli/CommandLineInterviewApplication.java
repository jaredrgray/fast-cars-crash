package com.tesla.interview.application.cli;

import com.beust.jcommander.JCommander;
import com.google.common.collect.Lists;
import com.tesla.interview.application.InterviewApplication;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class CommandLineInterviewApplication {

  private static final String OUTPUT_FILE_FORMAT = "output-file-%d.csv";

  public static void main(String[] args) {
    CommandLineInterviewApplication app = new CommandLineInterviewApplication(args);
    app.execute();
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
    commander.parse(args);
  }

  private void execute() {
    if (!parsedArguments.isHelpCommand) {
      // build output file list
      Path outputDirectory = Paths.get(parsedArguments.outputDirectory);
      List<String> outputFilePaths = Lists.newArrayList();
      for (int i = 1; i <= outputFilePaths.size(); i++) {
        String outputFileName = String.format(OUTPUT_FILE_FORMAT, i);
        Path outputFilePath = outputDirectory.resolve(outputFileName);
        outputFilePaths.add(outputFilePath.toString());
      }

      // construct app
      InterviewApplication app = new InterviewApplication(parsedArguments.numWriteThreads,
          -1 /* TODO: maxFileHandles */, outputFilePaths, parsedArguments.inputFilePath);
      app.call();
    } else {
      commander.usage();
    }
  }
}
