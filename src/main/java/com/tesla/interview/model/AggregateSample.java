package com.tesla.interview.model;

/**
 * An aggregation of several {@link MeasurementSample} instances.
 */
public class AggregateSample {

  private final long timestamp;

  private final String id;

  private final int aggregateValue;

  /**
   * Constructor.
   * 
   * @param timestamp number of milliseconds since January 1, 1970:UTC
   * @param id unique identifier for the asset
   * @param aggregateValue aggregation value of constituent samples
   */
  public AggregateSample(long timestamp, String id, int aggregateValue) {
    this.timestamp = timestamp;
    this.id = id;
    this.aggregateValue = aggregateValue;
  }

  public int getAggregateValue() {
    return aggregateValue;
  }

  public String getId() {
    return id;
  }

  public long getTimestamp() {
    return timestamp;
  }

}
