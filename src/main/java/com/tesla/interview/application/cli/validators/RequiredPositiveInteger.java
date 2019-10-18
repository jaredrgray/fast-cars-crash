package com.tesla.interview.application.cli.validators;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;

public class RequiredPositiveInteger implements IValueValidator<Integer> {

  @Override
  public void validate(String name, Integer value) throws ParameterException {
    if (value == null) {
      throw new ParameterException(String.format("%s is required", name));
    }
    if (value.intValue() <= 0) {
      throw new ParameterException(
          String.format("%s must be a positive integer (was %d)", name, value.intValue()));
    }
  }

}
