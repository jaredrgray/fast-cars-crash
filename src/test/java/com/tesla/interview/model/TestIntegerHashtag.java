package com.tesla.interview.model;

import static java.util.Map.Entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.Maps;
import com.tesla.interview.model.IntegerHashtag;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestIntegerHashtag {

  private final Map<String, Integer> tagToExpectedValue;
  private final Map<Integer, String> valueToExpectedTag;

  private TestIntegerHashtag() {
    tagToExpectedValue = Maps.newHashMap();
    tagToExpectedValue.put("#one", 1);
    tagToExpectedValue.put("#two", 2);
    tagToExpectedValue.put("#three", 3);
    tagToExpectedValue.put("#four", 4);
    tagToExpectedValue.put("#five", 5);
    tagToExpectedValue.put("#six", 6);
    tagToExpectedValue.put("#seven", 7);
    tagToExpectedValue.put("#eight", 8);
    tagToExpectedValue.put("#nine", 9);
    tagToExpectedValue.put("#ten", 10);

    valueToExpectedTag = Maps.newHashMap();
    for (Entry<String, Integer> e : tagToExpectedValue.entrySet()) {
      valueToExpectedTag.put(e.getValue(), e.getKey());
    }
  }

  @Test
  void testConstructorsAndGetters() {
    for (IntegerHashtag underTest : IntegerHashtag.values()) {
      assertTrue(tagToExpectedValue.containsKey(underTest.getTag()));
      assertEquals(tagToExpectedValue.get(underTest.getTag()), underTest.getValue());

      assertTrue(valueToExpectedTag.containsKey(underTest.getValue()));
      assertEquals(valueToExpectedTag.get(underTest.getValue()), underTest.getTag());
    }
  }

  @Test
  void testInvalidConstructor() {
    try {
      IntegerHashtag.valueOf("invalid");
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("invalid"));
    }
  }
}
