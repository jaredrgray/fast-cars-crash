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

package com.tesla.interview.application.cli.validators;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.beust.jcommander.ParameterException;
import org.junit.jupiter.api.Test;

public class TestRequiredPositiveInteger {

  private static final String MUST_BE_A_POSITIVE_INTEGER = "must be a positive integer";
  private static final String PARAM_NAME = "parameterName";

  @Test
  public void testEmptyValueFails() {
    RequiredPositiveInteger underTest = new RequiredPositiveInteger();
    try {
      underTest.validate(PARAM_NAME, null /* value */);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains("cannot be null"));
    }
  }

  @Test
  public void testNegativeValueFails() {
    RequiredPositiveInteger underTest = new RequiredPositiveInteger();
    try {
      underTest.validate(PARAM_NAME, -1 /* value */);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains(MUST_BE_A_POSITIVE_INTEGER));
    }
  }

  @Test
  public void testPositiveValueSucceeds() {
    RequiredPositiveInteger underTest = new RequiredPositiveInteger();
    underTest.validate(PARAM_NAME, 1 /* value */);
  }

  @Test
  public void testZeroValueFails() {
    RequiredPositiveInteger underTest = new RequiredPositiveInteger();
    try {
      underTest.validate(PARAM_NAME, 0 /* value */);
      fail("Expected ParameterException");
    } catch (ParameterException e) {
      assertTrue(e.getMessage().contains(MUST_BE_A_POSITIVE_INTEGER));
    }
  }

}
