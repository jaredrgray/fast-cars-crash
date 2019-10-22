package org.tesla.interview.tests;

import static org.tesla.interview.tests.IntegrationTestSuite.INTEGRATION_TEST_TAG;

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
@ExcludeTags(INTEGRATION_TEST_TAG)
public class UnitTestSuite {

}
