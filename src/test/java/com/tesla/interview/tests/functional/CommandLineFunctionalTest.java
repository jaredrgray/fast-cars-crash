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

package com.tesla.interview.tests.functional;

import static com.tesla.interview.tests.FunctionalTestSuite.FUNCTIONAL_TEST_TAG;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.tesla.interview.application.cli.CommandLineInterviewApplication;
import com.tesla.interview.tests.InterviewTestCase;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(FUNCTIONAL_TEST_TAG)
public class CommandLineFunctionalTest extends InterviewTestCase {

  private static final String CSV_EXTENSION = ".csv";
  private static final String OUTPUT_FILE = "output-file-";
  private static final int QUEUE_SIZE = 100;
  private static final int NUM_PARTITIONS = 20;
  private static final int NUM_THREADS = 5;
  private static final Path PACKAGE_DIR = Paths
      .get(CommandLineFunctionalTest.class.getPackage().getName().replace(".", File.separator));
  private static final Path INPUT_SAMPLES_TXT = PACKAGE_DIR.resolve("input_samples.txt");

  @Test
  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
  void testEndToEnd(TestInfo testInfo) throws IOException {
    File inputFile = getInputFile(INPUT_SAMPLES_TXT);
    File outputDir = createTempDir(testInfo).toFile();
    String[] args = new String[] {"-i", inputFile.getPath(), "-o", outputDir.getPath(), "-p",
        String.valueOf(NUM_PARTITIONS), "-w", String.valueOf(NUM_THREADS)};
    CommandLineInterviewApplication app = new CommandLineInterviewApplication(args, QUEUE_SIZE);
    CommandLineInterviewApplication.executeWrapper(app);

    Map<Integer, Integer> partitionNumToNumLinesExpected = Maps.newHashMap();
    partitionNumToNumLinesExpected.put(1, 7572);
    partitionNumToNumLinesExpected.put(2, 7387);
    partitionNumToNumLinesExpected.put(3, 7568);
    partitionNumToNumLinesExpected.put(4, 7554);
    partitionNumToNumLinesExpected.put(5, 7440);
    partitionNumToNumLinesExpected.put(6, 7389);
    partitionNumToNumLinesExpected.put(7, 7484);
    partitionNumToNumLinesExpected.put(8, 7604);
    partitionNumToNumLinesExpected.put(9, 7379);
    partitionNumToNumLinesExpected.put(10, 7389);
    partitionNumToNumLinesExpected.put(11, 7597);
    partitionNumToNumLinesExpected.put(12, 7568);
    partitionNumToNumLinesExpected.put(13, 7489);
    partitionNumToNumLinesExpected.put(14, 7599);
    partitionNumToNumLinesExpected.put(15, 7577);
    partitionNumToNumLinesExpected.put(16, 7504);
    partitionNumToNumLinesExpected.put(17, 7534);
    partitionNumToNumLinesExpected.put(18, 7509);
    partitionNumToNumLinesExpected.put(19, 7402);
    partitionNumToNumLinesExpected.put(20, 7455);

    String[] filesCreated = outputDir.list();
    assertEquals(NUM_PARTITIONS, filesCreated.length);
    for (String outputFile : filesCreated) {
      try {
        String partitionStr = outputFile.substring(OUTPUT_FILE.length(),
            outputFile.length() - CSV_EXTENSION.length());
        Integer partitionNum = Integer.valueOf(partitionStr);
        assertNotNull(partitionNum);
        assertTrue(partitionNumToNumLinesExpected.containsKey(partitionNum));
        int result = countLines(outputDir, outputFile);
        assertEquals(partitionNumToNumLinesExpected.get(partitionNum), result);
      } finally {
        assertTrue(outputDir.toPath().resolve(outputFile).toFile().delete());
      }
    }
  }

  private int countLines(File outputDir, String file) throws IOException {
    Reader input = Files.newBufferedReader(outputDir.toPath().resolve(file), Charsets.UTF_8);
    LineNumberReader count = new LineNumberReader(input);
    try {
      while (count.skip(Long.MAX_VALUE) > 0) {
      }
    } finally {
      try {
        input.close();
      } finally {
        count.close();
      }
    }
    return count.getLineNumber();
  }

  private static File getInputFile(Path path) {
    URL resource = Resources.getResource(path.toString());
    if (resource != null) {
      return new File(resource.getPath());
    } else {
      throw new IllegalArgumentException("could not open input file");
    }
  }

}
