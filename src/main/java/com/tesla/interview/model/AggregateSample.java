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
