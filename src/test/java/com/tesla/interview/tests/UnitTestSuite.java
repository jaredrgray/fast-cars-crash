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

import static com.tesla.interview.tests.FunctionalTestSuite.FUNCTIONAL_TEST_TAG;
import static com.tesla.interview.tests.IntegrationTestSuite.INTEGRATION_TEST_TAG;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.ExcludeTags;
import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.runner.RunWith;

/**
 * The unit test suite consists of all unit tests. By convention, unit tests class names begin with
 * "Test".
 */
@RunWith(JUnitPlatform.class)
@SelectPackages("com.tesla.interview.application")
@IncludeClassNamePatterns({"^.+[.]Test.*$"})
@ExcludeTags({FUNCTIONAL_TEST_TAG, INTEGRATION_TEST_TAG})
public class UnitTestSuite {

}
