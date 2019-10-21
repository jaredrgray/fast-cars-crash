package com.tesla.interview.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.tesla.interview.model.AggregateSample;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class TestAggregateSample {

  @Test
  void testConstructorAndGetters() {
    int aggregateValue = 0;
    String id = UUID.randomUUID().toString();
    int partitionNo = 1;
    long timestamp = 2L;
    AggregateSample underTest = new AggregateSample(aggregateValue, id, partitionNo, timestamp);

    assertEquals(aggregateValue, underTest.getAggregateValue());
    assertEquals(id, underTest.getAssetId());
    assertEquals(partitionNo, underTest.getPartitionNo());
    assertEquals(timestamp, underTest.getTimestamp());
  }
}
