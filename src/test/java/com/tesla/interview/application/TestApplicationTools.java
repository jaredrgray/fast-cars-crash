package com.tesla.interview.application;

import static com.tesla.interview.application.ApplicationTools.consoleTrace;
import static com.tesla.interview.application.ApplicationTools.logTrace;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.beust.jcommander.internal.Console;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

public class TestApplicationTools {

  @Test
  void testConsoleTraceSimple() {
    Console mockConsole = mock(Console.class);
    Exception onion = new Exception();
    consoleTrace(mockConsole, onion);
    verify(mockConsole, times(onion.getStackTrace().length + 1)).println(anyString());
  }

  @Test
  void testConsoleTraceWithNested() {
    Console mockConsole = mock(Console.class);
    Exception onion = new Exception(new Exception(new Exception()));
    consoleTrace(mockConsole, onion);
    verify(mockConsole, times(onion.getStackTrace().length + 1)).println(anyString());
  }

  @Test
  void testLogTraceSimple() {
    Logger mockLog = mock(Logger.class);
    Level logLevel = Level.TRACE;
    Exception onion = new Exception();
    logTrace(mockLog, logLevel, onion);
    verify(mockLog, times(onion.getStackTrace().length + 1)).log(eq(logLevel), anyString());
  }

  @Test
  void testLogTraceWithNested() {
    Logger mockLog = mock(Logger.class);
    Level logLevel = Level.DEBUG;
    Exception onion = new Exception(new Exception(new Exception()));
    logTrace(mockLog, logLevel, onion);
    verify(mockLog, times(onion.getStackTrace().length + 1)).log(eq(logLevel), anyString());
  }
}
