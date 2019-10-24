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

package com.tesla.interview.tests;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.IncludeTags;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.runner.RunWith;

/**
 * The unit test suite consists of all functional tests. By convention, functional test class
 * names end with "FunctionalTest" <strong>AND</strong> are tagged with
 * {@link FunctionalTestSuite#FUNCTIONAL_TEST_TAG}.
 */
@RunWith(JUnitPlatform.class)
@SelectPackages("com.tesla.interview")
@IncludeClassNamePatterns({"^.+FunctionalTest$"})
@IncludeTags(FunctionalTestSuite.FUNCTIONAL_TEST_TAG)
public class FunctionalTestSuite {

  /** Tag to use for all integration tests. */
  public static final String FUNCTIONAL_TEST_TAG = "FunctionalTest";

}
