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
