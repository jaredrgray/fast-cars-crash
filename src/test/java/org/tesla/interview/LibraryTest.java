package org.tesla.interview;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class LibraryTest {
  @Test
  public void testSomeLibraryMethod() {
    Library classUnderTest = new Library();
    assertTrue(classUnderTest.someLibraryMethod(), "someLibraryMethod should return 'true'");
  }
}
