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

package com.tesla.interview.model;

import com.google.common.collect.Maps;
import java.util.Map;

/**
 * A hashtag and its associated integer value, e.g. "#six" -> six.
 */
public enum IntegerHashtag {

  /* empty comments present below to please the code formatter */
  ONE("#one", 1), //
  TWO("#two", 2), //
  THREE("#three", 3), //
  FOUR("#four", 4), //
  FIVE("#five", 5), //
  SIX("#six", 6), //
  SEVEN("#seven", 7), //
  EIGHT("#eight", 8), //
  NINE("#nine", 9), //
  TEN("#ten", 10), //
  ;

  private static final Map<String, IntegerHashtag> tagToEnum;
  static {
    tagToEnum = Maps.newHashMap();
    for (IntegerHashtag e : IntegerHashtag.values()) {
      tagToEnum.put(e.getTag(), e);
    }
  }

  /**
   * Construct an enum from a tag.
   * 
   * @param tag tag whose associated enum to return
   * @return non-null enum associated with tag
   */
  public static IntegerHashtag fromTag(String tag) {
    if (tagToEnum.containsKey(tag)) {
      return tagToEnum.get(tag);
    } else {
      throw new IllegalArgumentException("No such tag: " + tag);
    }
  }

  private final String tag;
  private final int value;

  private IntegerHashtag(String tag, int value) {
    this.tag = tag;
    this.value = value;
  }

  public String getTag() {
    return this.tag;
  }

  public int getValue() {
    return this.value;
  }
}
