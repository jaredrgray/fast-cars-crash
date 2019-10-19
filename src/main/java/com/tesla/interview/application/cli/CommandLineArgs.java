package com.tesla.interview.application.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.tesla.interview.application.cli.validators.ExistingReadableDirectory;
import com.tesla.interview.application.cli.validators.ExistingReadableFile;
import com.tesla.interview.application.cli.validators.RequiredPositiveInteger;

@Parameters(separators = " =")
public class CommandLineArgs {

  @Parameter(names = {"--numPartitions", "-p"},
      description = "Number of partitions in the input file",
      validateValueWith = RequiredPositiveInteger.class)
  int numPartitions = 1;

  @Parameter(names = {"--numWriteThreads", "-w"}, validateValueWith = RequiredPositiveInteger.class,
      description = "Number of threads to use for writing output files")
  int numWriteThreads = 1;

  @Parameter(names = {"--inputFile", "-i"}, description = "File system path to the input file",
      validateValueWith = ExistingReadableFile.class)
  String inputFile;

  @Parameter(names = {"--outputDirectory", "-o"},
      description = "Path to the directory in which output files shall be placed",
      validateValueWith = ExistingReadableDirectory.class)
  String outputDirectory;

  @Parameter(names = {"--help", "-h"}, description = "Display usage")
  boolean isHelpCommand;
}
