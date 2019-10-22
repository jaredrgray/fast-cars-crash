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

package com.tesla.interview.application;

import com.beust.jcommander.internal.Console;
import java.util.function.Consumer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

/**
 * A collection of static functions that are useful to the application.
 */
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

    if (th.getCause() == null) {
      // base case: got outermost exception
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
