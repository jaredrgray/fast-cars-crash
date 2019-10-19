package com.tesla.interview.application.cli.validators;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ExistingReadableDirectory implements IValueValidator<String> {

  @Override
  public void validate(String name, String value) throws ParameterException {
    if (value != null) {
      validatePath(name, Paths.get(value));
    } else {
      throw new ParameterException(String.format("%s cannot be null", name));
    }
  }

  /**
   * Validate that the path is an existing readable directory.
   * <p>
   * Package-visible for unit tests.
   * </p>
   * 
   * @param name variable name
   * @param path path to validate
   */
  void validatePath(String name, Path path) {
    if (path == null || path.toFile() == null || !path.toFile().exists()
        || !path.toFile().isDirectory() || !path.toFile().canRead()) {
      throw new ParameterException(
          String.format("%s must be an existing, readable directory (provided: %s)", name, path));
    }
  }

}
