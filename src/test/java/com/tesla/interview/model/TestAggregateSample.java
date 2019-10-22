package com.tesla.interview.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.tesla.interview.model.AggregateSample;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class TestAggregateSample {

  @Test
  void testConstructorAndGetters() {
    int aggregateValue = 0;
    String assetId = UUID.randomUUID().toString();
    int partitionNo = 1;
    long timestamp = 2L;
    AggregateSample underTest =
        new AggregateSample(aggregateValue, assetId, partitionNo, timestamp);

    assertEquals(aggregateValue, underTest.getAggregateValue());
    assertEquals(assetId, underTest.getAssetId());
    assertEquals(partitionNo, underTest.getPartitionNo());
    assertEquals(timestamp, underTest.getTimestamp());
  }
}
