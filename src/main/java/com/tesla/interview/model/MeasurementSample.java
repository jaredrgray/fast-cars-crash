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

import static org.apache.logging.log4j.LogManager.getLogger;

import com.google.common.collect.Sets;
import com.tesla.interview.application.Generated;
import java.util.Set;
import org.apache.logging.log4j.Logger;

/**
 * A data point collected from an Internet of Things (IoT) device.
 */
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
      throw new IllegalArgumentException("Insufficient number of fields for sample");
    }

    // parse first three fields within string
    final long timestamp;
    final int partitionNo;
    final String id;

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
      throw new IllegalArgumentException("Third field (asset identifier) cannot be empty");
    }

    if (fields.length < 4) {
      throw new IllegalArgumentException("Fourth field (hashtags) cannot be empty");
    }

    // construct the remaining field, a comma-delimited set of hashtags
    final Set<IntegerHashtag> hashtags = Sets.newHashSet();
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
  private final String assetId;
  private final int partitionNo;
  private final long timestamp;

  /**
   * Canonical constructor.
   * 
   * @param timestamp number of milliseconds since January 1, 1970:UTC
   * @param partitionNo TODO (I can't define this well based on specification provided)
   * @param assetId unique identifier for asset
   * @param hashtags tags associated with this sample
   */
  public MeasurementSample(long timestamp, int partitionNo, String assetId,
      Set<IntegerHashtag> hashtags) {
    this.timestamp = timestamp;
    this.partitionNo = partitionNo;
    this.assetId = assetId;
    this.hashtags = hashtags;
  }

  @Override
  @SuppressWarnings("checkstyle:NeedBraces")
  @Generated
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MeasurementSample other = (MeasurementSample) obj;
    if (assetId == null) {
      if (other.assetId != null)
        return false;
    } else if (!assetId.equals(other.assetId))
      return false;
    if (hashtags == null) {
      if (other.hashtags != null)
        return false;
    } else if (!hashtags.equals(other.hashtags))
      return false;
    if (partitionNo != other.partitionNo)
      return false;
    if (timestamp != other.timestamp)
      return false;
    return true;
  }

  public String getAssetId() {
    return assetId;
  }

  public Set<IntegerHashtag> getHashtags() {
    return hashtags;
  }

  public int getPartitionNo() {
    return partitionNo;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  @Generated
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((assetId == null) ? 0 : assetId.hashCode());
    result = prime * result + ((hashtags == null) ? 0 : hashtags.hashCode());
    result = prime * result + partitionNo;
    result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    String prefix = String.join(FIELD_SEPARATOR, String.valueOf(timestamp),
        String.valueOf(partitionNo), assetId);
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
