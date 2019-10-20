package com.tesla.interview.model;

/**
 * An aggregation of several {@link MeasurementSample} instances.
 */
public class AggregateSample {

  private final int aggregateValue;
  private final String id;
  private final int partitionNo;
  private final long timestamp;
  
  /**
   * Constructor.
   * 
   * @param aggregateValue aggregation value of constituent samples
   * @param id unique identifier for the asset
   * @param partitionNo TODO (need better description from specification)
   * @param timestamp number of milliseconds since January 1, 1970:UTC
   */
  public AggregateSample(int aggregateValue, String id, int partitionNo, long timestamp) {
    this.aggregateValue = aggregateValue;
    this.id = id;
    this.partitionNo = partitionNo;
    this.timestamp = timestamp;
  }

  public int getAggregateValue() {
    return aggregateValue;
  }

  public String getId() {
    return id;
  }

  public int getPartitionNo() {
    return partitionNo;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "AggregateSample [aggregateValue=" + aggregateValue + ", id=" + id + ", partitionNo="
        + partitionNo + ", timestamp=" + timestamp + "]";
  }

}
