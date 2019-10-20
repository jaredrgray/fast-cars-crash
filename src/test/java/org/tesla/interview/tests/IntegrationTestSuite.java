package org.tesla.interview.tests;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.IncludeTags;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.runner.RunWith;

/**
 * The unit test suite consists of all unit tests. By convention, these tests classes names' end
 * with "IntegrationTest" <strong>AND</strong> are tagged with
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
