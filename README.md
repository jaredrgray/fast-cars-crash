# Fast Cars Crash

This program aggregates Internet of Things (Io)T sampling data, represented as a single input text file.  We output the resulting aggregations to a series of output text files.

## System Requirements

This distribution contains a [Gradle Wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html) that can be used to conveniently build and run the program.  You do not need to install Gradle, as the wrapper will download and run an appropriate Gradle version automatically.  Even supposing you do have Gradle installed, we recommend using the wrapper instead.

To run Gradle and to build the application, a [Java Development Kit](https://jdk.java.net/) is required.  This application currently supports Java SE8 and above.

## Usage

Execute the following command within a shell to display the program's built-in usage:

```
 $ ./gradlew run --args="--help"
```

See below an example output:

```
Usage: <main class> [options]
  Options:
    --help, -h
      Display usage
      Default: false
  * --inputFile, -i
      File system path to the input file
  * --numPartitions, -p
      Number of partitions in the input file
    --numWriteThreads, -w
      Number of threads to use for writing output files
      Default: 1
  * --outputDirectory, -o
      Path to the directory in which output files shall be placed
```

The program's parameters correspond to a specification for input and output (to be detailed in future sections).  To help users struggling with syntax, the program displays context-specific error messages explaining why the user's input is invalid.

### Example Usage

Suppose we have a text file called `input.txt` containing only partitions 1-4 and an output directory named `outputDir`.  We can execute the command below within a shell to write the four output files:

```
 $ ./gradlew run --args="-i $HOME/Desktop/input.txt -o $HOME/Desktop/outputDir -p 4"
```

Some tasks of note include:

1. unitTest: execute all unit tests
2. integrationTest: execute all integration tests
3. jacocoTestReport: output code coverage in HTML format to `./build/jacocoHtml`
4. check: validate code for test coverage and against programming style requirements

## Input Text File Format

The expected format of each line in the input file is a comma-separated list consisting of the following elements from an IoT device:

1. Timestamp
1. Partition number
1. Asset identifier
1. Hashtags

For example:

> 1505233687037,4,fe52fa24-4527-4dfd-be87-348812e0c736,#seven,#three,#six

Here, the timestamp is `1505233687037`, the partition number is 4, and the asset identifier is a UUID.

### Hash Tags

As can be seen in the example, the hashtags field may contain multiple comma-separated values.  Only the following values are considered valid:

* #one
* #two
* #three
* #four
* #five
* #six
* #seven
* #eight
* #nine
* #ten

All other hashtags and non-hashtags at the end of an input line will be ignored.  These shall be logged to the console for auditing purposes.

### Field Types

Our program is agnostic to the data types representing the timestamp and asset identifier.  The partition number, however, is expected always to be a natural number.


### Line Termination

We interpret following characters sequences as end-of-line signifiers:

1. Windows newline (`\r\n`)
1. UNIX newline (`\n`) 
1. Carriage return (`\r`)
1. End-of-file

The longest applicable termination sequences will be consumed whenever multiple termination sequences are present.

## Output Text File(s) Format 

Our program produces its output in the form of one or more output text files.  There will be one output text file for each partition number.  The files shall be named output-file-*n*.csv, where *n* is a partition number.

Each line in the output file is a comma-separated list consisting of the following elements:

1. Timestamp
1. Asset id
1. Aggregate value

We define the aggregate value as the sum of the integers corresponding to the hashtags from an input sample.  Let us again consider the example line from input section.  The aggregate value for this sample is 7 + 3 + 6 = 16.

See below the full line written to output-4.txt (so chosen because 4 is the partition number from the sample):

> 1505233687037,fe52fa24-4527-4dfd-be87-348812e0c736,16

### Line Termination

The end-of-line character sequence written to the output file will depend on the platform on which this program runs.  On most UNIX-like operating systems, the termination character will be `\n`.

### Ordering

Data written to the output files shall appear in the order in which they were read from the input file.  Consider two data samples *A* and *B* with the same partition number.  The output file **must** contain a line corresponding to *A* prior to the line derived from *B*.

## Development

We have customized the Gradle environment to  support advanced functionality for developers.  Execute the following command within a shell to see all tasks supported in this environment: 

```
 $ ./gradlew tasks
```
