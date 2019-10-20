package com.tesla.interview.application;

import com.beust.jcommander.internal.Console;
import java.util.function.Consumer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

public class ApplicationTools {

  /**
   * Print a trace of the supplied exception to the console.
   * 
   * @param console console to which to print
   * @param th exception to print
   */
  public static void consoleTrace(Console console, Throwable th) {
    Consumer<String> printFun = (String message) -> console.println(message);
    printTrace(printFun, th);
  }

  /**
   * Print a trace of the supplied exception to the log.
   * 
   * @param logger logger to which to print
   * @param logLevel level of log print statement
   * @param th exception to print
   */
  public static void logTrace(Logger logger, Level logLevel, Throwable th) {
    Consumer<String> printFun = (String message) -> logger.log(logLevel, message);
    printTrace(printFun, th);
  }

  /**
   * Print a stack trace of the input exception using the provided print function.
   * 
   * @param printFun function that prints
   * @param th exception to trace
   */
  private static void printTrace(Consumer<String> printFun, Throwable th) {

    // base case: got outermost exception
    if (th.getCause() == null) {
      printFun.accept("Stack trace:");
      StackTraceElement[] elements = th.getStackTrace();
      int numDigits = String.valueOf(elements.length).length();

      // print to console
      String depthFormat = "%" + "0" + numDigits + "d";
      for (int i = 1; i <= elements.length; i++) {
        String depth = String.format(depthFormat, i);
        // @formatter:off
        printFun.accept(new StringBuilder()
            .append(depth)
            .append(": ")
            .append(elements[i - 1].toString())
            .toString());
        // @formatter:on
      }
    } else {
      // recurse
      printTrace(printFun, th.getCause());
    }
  }

}
