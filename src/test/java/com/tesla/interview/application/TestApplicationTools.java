/*******************************************************************************
 * Copyright (c) 2019 Jared R Gray
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License. You may obtain a copy of the License at
 *  
 *  https://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *******************************************************************************/
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
