package com.tesla.interview.model;

import static org.apache.logging.log4j.LogManager.getLogger;

import com.google.common.collect.Sets;
import com.tesla.interview.application.Generated;
import java.util.Set;
import org.apache.logging.log4j.Logger;

public class MeasurementSample {

  private static final String FIELD_SEPARATOR = ",";

  private static final Logger LOG = getLogger(MeasurementSample.class);

  /**
   * Serialize a {@link MeasurementSample} from a String.
   * 
   * @param sampleString string to serialize
   * @return corresponding {@link MeasurementSample}
   */
  public static MeasurementSample fromString(String sampleString) {
    if (sampleString == null || sampleString.isEmpty()) {
      throw new IllegalArgumentException("sampleString cannot be empty");
    }

    String[] fields = sampleString.split(FIELD_SEPARATOR);
    if (fields.length < 3) {
      throw new IllegalArgumentException("Incomplete sample provided");
    }

    // variables to parse from string
    final long timestamp;
    final int partitionNo;
    final String id;
    final Set<IntegerHashtag> hashtags = Sets.newHashSet();

    try {
      timestamp = Long.parseLong(fields[0]);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("First field (timestamp) must be a number");
    }

    try {
      partitionNo = Integer.parseInt(fields[1]);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Second field (partitionNo) must be a numberr");
    }

    id = fields[2];
    if (id.isEmpty()) {
      throw new IllegalArgumentException("Third field (id) cannot be empty");
    }

    if (fields.length < 4) {
      throw new IllegalArgumentException("Fourth field (hashtags) cannot be empty");
    }

    for (int i = 3; i < fields.length; i++) {
      try {
        IntegerHashtag hashtag = IntegerHashtag.fromTag(fields[i]);
        hashtags.add(hashtag);
      } catch (RuntimeException e) {
        // we ignore anything that is invalid rather than blow up the application
        LOG.warn(String.format("unable to parse hashtag -- fieldNo: %d, hashtag: %s ", i + 1,
            fields[i]));
      }
    }

    return new MeasurementSample(timestamp, partitionNo, id, hashtags);
  }

  private final Set<IntegerHashtag> hashtags;
  private final String id;
  private final int partitionNo;
  private final long timestamp;

  /**
   * Constructor.
   * 
   * @param timestamp number of milliseconds since January 1, 1970:UTC
   * @param partitionNo TODO (I can't define this well based on specification provided)
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

  @SuppressWarnings("checkstyle:NeedBraces")
  @Generated
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MeasurementSample other = (MeasurementSample) obj;
    if (hashtags == null) {
      if (other.hashtags != null)
        return false;
    } else if (!hashtags.equals(other.hashtags))
      return false;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (partitionNo != other.partitionNo)
      return false;
    if (timestamp != other.timestamp)
      return false;
    return true;
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

  @Override
  public String toString() {
    String prefix =
        String.join(FIELD_SEPARATOR, String.valueOf(timestamp), String.valueOf(partitionNo), id);
    StringBuilder suffix = new StringBuilder();
    for (IntegerHashtag hashtag : hashtags) {
      if (suffix.length() > 0) {
        suffix.append(FIELD_SEPARATOR);
      }
      suffix.append(hashtag.getTag());
    }
    return prefix + FIELD_SEPARATOR + suffix;
  }
}
