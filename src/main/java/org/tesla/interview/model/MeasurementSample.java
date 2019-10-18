package org.tesla.interview.model;

import java.util.Set;

public class MeasurementSample {

  private final long timestamp;

  private final int partitionNo;

  private final String id;

  private final Set<IntegerHashtag> hashtags;

  /**
   * Constructor.
   * 
   * @param timestamp number of milliseconds since January 1, 1970:UTC
   * @param partitionNo TODO (I really can't define this based on specification provided)
   * @param id unique identifier
   * @param hashtags tags associated with this sample
   */
  public MeasurementSample(long timestamp, int partitionNo, String id,
      Set<IntegerHashtag> hashtags) {
    this.timestamp = timestamp;
    this.partitionNo = partitionNo;
    this.id = id;
    this.hashtags = hashtags;
  }

  public Set<IntegerHashtag> getHashtags() {
    return hashtags;
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
}
