package com.tesla.interview.application.cli;

import com.beust.jcommander.Parameter;

public class CommandLineArgs {
  
  @Parameter(names = {"--numPartitions", "-p"},
      description = "Number of partitions in the input file")
  int numPartitions;
  
  @Parameter(names = {"--numWriteThreads", "-w"},
      description = "Number of threads to use for writing output files")
  int numWriteThreads;
  
  @Parameter(names = {"--inputFile", "-i"},
      description = "File system path to the input file")
  String inputFilePath;
  
  @Parameter(names = {"--outputDirectory", "-o"},
      description = "Path to the directory in which output files shall be placed")
  String outputDirectory;
  
  @Parameter(names = {"--help", "-h"},
      description = "Display usage")
  boolean isHelpCommand;
}
