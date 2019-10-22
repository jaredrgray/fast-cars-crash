package org.tesla.interview.tests;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.IncludeTags;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.runner.RunWith;

/**
 * The unit test suite consists of all integration tests. By convention, integration tests class
 * names end with "IntegrationTest" <strong>AND</strong> are tagged with
 * {@link IntegrationTestSuite#INTEGRATION_TEST_TAG}.
 */
@RunWith(JUnitPlatform.class)
@SelectPackages("com.tesla.interview")
@IncludeClassNamePatterns({"^.+IntegrationTest$"})
@IncludeTags(IntegrationTestSuite.INTEGRATION_TEST_TAG)
public class IntegrationTestSuite {

  /** Tag to use for all integration tests. */
  public static final String INTEGRATION_TEST_TAG = "IntegrationTest";

}
