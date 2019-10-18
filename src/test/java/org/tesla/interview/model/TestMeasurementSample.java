package org.tesla.interview.model;

import static com.tesla.interview.model.IntegerHashtag.EIGHT;
import static com.tesla.interview.model.IntegerHashtag.FIVE;
import static com.tesla.interview.model.IntegerHashtag.FOUR;
import static com.tesla.interview.model.IntegerHashtag.NINE;
import static com.tesla.interview.model.IntegerHashtag.ONE;
import static com.tesla.interview.model.IntegerHashtag.SEVEN;
import static com.tesla.interview.model.IntegerHashtag.SIX;
import static com.tesla.interview.model.IntegerHashtag.TEN;
import static com.tesla.interview.model.IntegerHashtag.THREE;
import static com.tesla.interview.model.IntegerHashtag.TWO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.Sets;
import com.tesla.interview.model.IntegerHashtag;
import com.tesla.interview.model.MeasurementSample;
import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestMeasurementSample {

  static enum FromStringPositiveTestCase {
    // @formatter:off
    FIRST(
        "1505233687023,2,3c8f3f69-f084-4a0d-b0a7-ea183fabceef,#eight,#six,#five",
        1505233687023L,
        2, 
        "3c8f3f69-f084-4a0d-b0a7-ea183fabceef", 
        Sets.newHashSet(EIGHT, FIVE, SIX)
    ), SECOND(
        "1505233687036,2,ead3d58b-85f3-4f54-8e1e-b0a21ae99a0d,#five,#eight,#seven",
        1505233687036L, 
        2, 
        "ead3d58b-85f3-4f54-8e1e-b0a21ae99a0d", 
        Sets.newHashSet(FIVE, EIGHT, SEVEN)
    ), THIRD(
        "1505233687037,2,345f7eb1-bf33-40c1-82a4-2f91c658803f" 
            + ",#two,#eight,#four,#nine,#ten,#three,#one,#seven,#six,#five",
        1505233687037L, 
        2, 
        "345f7eb1-bf33-40c1-82a4-2f91c658803f",
        Sets.newHashSet(TWO, EIGHT, FOUR, NINE, TEN, THREE, ONE, SEVEN, SIX, FIVE)
    ), FOURTH(
        // ignore duplicate tags
        "1505233687037,2,345f7eb1-bf33-40c1-82a4-2f91c658803f"
            + ",#one,#one,#one,#one,#one,#one,#one",
        1505233687037L, 
        2, 
        "345f7eb1-bf33-40c1-82a4-2f91c658803f",
        Sets.newHashSet(ONE)
    ), FIFTH(
        // ignore bad tags
        "1505233687037,2,345f7eb1-bf33-40c1-82a4-2f91c658803f,"
            + "#one,,two,three,more badness!,#eleven",
        1505233687037L, 
        2, 
        "345f7eb1-bf33-40c1-82a4-2f91c658803f",
        Sets.newHashSet(ONE)
    ), 
    ;
    // @formatter:on

    private String testString;
    private long expectedTimestamp;
    private int expectedPartition;
    private String expectedId;
    private Set<IntegerHashtag> expectedTags;

    FromStringPositiveTestCase(String testString, long timestamp, int partitionNo, String id,
        Set<IntegerHashtag> tags) {
      this.testString = testString;
      this.expectedTimestamp = timestamp;
      this.expectedPartition = partitionNo;
      this.expectedId = id;
      this.expectedTags = tags;
    }
  }

  @ParameterizedTest
  @EnumSource(FromStringPositiveTestCase.class)
  void testFromStringPositive(FromStringPositiveTestCase testCase) {
    MeasurementSample underTest = MeasurementSample.fromString(testCase.testString);
    assertEquals(testCase.expectedId, underTest.getId());
    assertEquals(testCase.expectedPartition, underTest.getPartitionNo());
    assertEquals(testCase.expectedTimestamp, underTest.getTimestamp());
    assertEquals(testCase.expectedTags.size(), underTest.getHashtags().size());
    for (IntegerHashtag expected : testCase.expectedTags) {
      assertTrue(underTest.getHashtags().contains(expected));
    }
  }

  static enum FromStringNegativeTestCase {
    // @formatter:off
    BAD_TIMESTAMP(
        "I am not a number,2,3c8f3f69-f084-4a0d-b0a7-ea183fabceef,#eight,#six,#five",
        IllegalArgumentException.class,
        "must be a number"
    ), BAD_PARTITION(
        "1505233687036,I am not a number,ead3d58b-85f3-4f54-8e1e-b0a21ae99a0d,#five,#eight,#seven",
        IllegalArgumentException.class,
        "must be a number"
    ), EMPTY_ID(
        "1505233687037,2,,#two,#eight,#four,#nine,#ten,#three,#one,#seven,#six,#five",
        IllegalArgumentException.class, 
        "cannot be empty"
    ), EMPTY_TAGS(
        "1505233687037,2,345f7eb1-bf33-40c1-82a4-2f91c658803f,",
        IllegalArgumentException.class, 
        "cannot be empty"
    ),
    ;
    // @formatter:on

    private String testString;
    private Class<? extends Exception> expectedException;
    private String exceptionMessageSubstring;

    FromStringNegativeTestCase(String testString, Class<? extends Exception> expectedException,
        String exceptionMessageSubstring) {
      this.testString = testString;
      this.expectedException = expectedException;
      this.exceptionMessageSubstring = exceptionMessageSubstring;
    }
  }

  @ParameterizedTest
  @EnumSource(FromStringNegativeTestCase.class)
  void testFromStringNegative(FromStringNegativeTestCase testCase) {
    try {
      MeasurementSample.fromString(testCase.testString);
      fail("Expected " + testCase.expectedException.getSimpleName());
    } catch (Exception e) {
      if (e.getClass() != testCase.expectedException) {
        fail("Expected " + testCase.expectedException.getSimpleName());
      } else {
        assertTrue(e.getMessage().contains(testCase.exceptionMessageSubstring));
      }
    }
  }
}
