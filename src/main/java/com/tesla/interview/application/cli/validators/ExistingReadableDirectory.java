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
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Ensures the file parameter corresponds to an existing directory that we have permission to read.
 */
public class ExistingReadableDirectory implements IValueValidator<String> {

  @Override
  public void validate(String name, String value) throws ParameterException {
    if (value != null) {
      Path path = Paths.get(value);
      validatePath(name, path);
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
