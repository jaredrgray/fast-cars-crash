package com.tesla.interview.model;

/**
 * An aggregation of several {@link MeasurementSample} instances.
 */
public class AggregateSample {

  private static final String FIELD_SEPARATOR = ",";

  private final int aggregateValue;
  private final String assetId;
  private final int partitionNo;
  private final long timestamp;

  /**
   * Constructor.
   * 
   * @param aggregateValue aggregation value of constituent samples
   * @param assetId unique identifier for the asset
   * @param partitionNo TODO (need better description from specification)
   * @param timestamp number of milliseconds since January 1, 1970:UTC
   */
  public AggregateSample(int aggregateValue, String assetId, int partitionNo, long timestamp) {
    this.aggregateValue = aggregateValue;
    this.assetId = assetId;
    this.partitionNo = partitionNo;
    this.timestamp = timestamp;
  }

  public int getAggregateValue() {
    return aggregateValue;
  }

  public String getAssetId() {
    return assetId;
  }

  public int getPartitionNo() {
    return partitionNo;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return String.join(FIELD_SEPARATOR, String.valueOf(timestamp), assetId,
        String.valueOf(aggregateValue));
  }

}
