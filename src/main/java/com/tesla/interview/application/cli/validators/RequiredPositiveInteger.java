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

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;

/**
 * Ensures the input integer is present as a non-negative and non-zero value.
 */
public class RequiredPositiveInteger implements IValueValidator<Integer> {

  @Override
  public void validate(String name, Integer value) throws ParameterException {
    if (value == null) {
      throw new ParameterException(String.format("%s cannot be null", name));
    }

    if (Integer.compare(value, 1) < 0) {
      throw new ParameterException(
          String.format("%s must be a positive integer (was %d)", name, value));
    }
  }

}
