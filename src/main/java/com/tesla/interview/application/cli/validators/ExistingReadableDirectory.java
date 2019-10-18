package com.tesla.interview.application.cli.validators;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ExistingReadableDirectory implements IValueValidator<String> {

  @Override
  public void validate(String name, String value) throws ParameterException {
    if (value != null) {
      Path p = Paths.get(value);
      if (p == null || p.toFile() == null || !p.toFile().exists() || !p.toFile().isDirectory()
          || !p.toFile().canRead()) {
        throw new ParameterException(String
            .format("%s must be an existing, readable directory (provided: %s)", name, value));
      }
    } else {
      throw new ParameterException(String.format("%s is required", name));
    }
  }

}
