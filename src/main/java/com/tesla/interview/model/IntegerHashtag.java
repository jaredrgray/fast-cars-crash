package com.tesla.interview.model;

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
  TEN("#ten", 10) //
  ;

  /** String representation of the hashtag. */
  private final String tag;

  /** Integer representation of the hashtag's value. */
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
