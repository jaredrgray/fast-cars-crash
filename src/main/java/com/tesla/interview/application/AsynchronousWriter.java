/*******************************************************************************
 * Copyright (c) 2019 Jared R Gray
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License. You may obtain a copy of the License at
 *  
 *  https://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *******************************************************************************/
package com.tesla.interview.application;

import static org.apache.logging.log4j.LogManager.getLogger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tesla.interview.io.AggregateSampleWriter;
import com.tesla.interview.model.AggregateSample;
import java.io.Closeable;
import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.logging.log4j.Logger;

/**
 * Asynchronously writes input {@link AggregateSample}s to an output file via
 * {@link AggregateSampleWriter#writeSample(AggregateSample)}.
 */
public class AsynchronousWriter implements Closeable {

  /**
   * Schedules writes asynchronously.
   */
  class WriteScheduler extends Thread {

    @Override
    public void run() {

      LOG.info(String.format("Starting scheduler -- numPartitions: %d", partitionNumToPath.size()));

      Instant lastPrintTime = Instant.MIN;
      int lastNumTasksScheduled = 0;
      int lastNumTasksCompleted = 0;
      while (!isClosed.get()) {
        scheduleTasks();

        // print status periodically
        if (Duration.between(lastPrintTime, Instant.now()).compareTo(PRINT_INTERVAL) > 0) {
          lastPrintTime = Instant.now();
          StringBuilder message = new StringBuilder("progress -- ");
          boolean doPrint = false;
          if (lastNumTasksScheduled < numWriteTasksScheduled.intValue()) {
            message.append(String.format("numWriteTasksScheduled: %d",
                numWriteTasksScheduled.intValue() - lastNumTasksScheduled));
            doPrint = true;
          }
          lastNumTasksScheduled = numWriteTasksScheduled.intValue();
          if (lastNumTasksCompleted < numWriteTasksCompleted.intValue()) {
            if (doPrint) {
              message.append(", ");
            }
            message.append(String.format("numCompleted: %d",
                numWriteTasksCompleted.intValue() - lastNumTasksCompleted));
            doPrint = true;
          }

          if (doPrint) {
            LOG.info(message.toString());
          }
          lastNumTasksCompleted = numWriteTasksCompleted.intValue();
        }
      }
    }

    /**
     * Use a condition variable to schedule tasks efficiently.
     */
    private void scheduleTasks() {
      bufferLock.lock();
      try {
        while (!isClosed.get() && bufferedWrites.isEmpty()) {
          try {
            bufferHasTask.await(pollDelay.toMillis(), TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }

        if (!isClosed.get()) {
          WriteTask nextTask = bufferedWrites.remove();
          Future<WriteTask> taskFuture = executor.submit(nextTask);
          nextTask.scheduledHook(taskFuture);
        }
      } finally {
        bufferLock.unlock();
      }
    }
  }

  /**
   * Encapsulates a request to append an {@link AggregateSample} to an output file.
   */
  class WriteTask implements Callable<WriteTask> {
    private AggregateSample sample;
    private AtomicReference<Future<WriteTask>> scheduled;

    /**
     * Canonical constructor.
     * 
     * @param sample sample to write
     */
    WriteTask(AggregateSample sample) {
      this.sample = sample;
      scheduled = new AtomicReference<Future<WriteTask>>(null /* initialValue */);
    }

    @Override
    public WriteTask call() {
      int partitionFromZero = sample.getPartitionNo() - 1;
      String path = partitionNumToPath.getOrDefault(partitionFromZero, null /* defaultValue */);
      AggregateSampleWriter writer = pathToWriter.getOrDefault(path, null /* defaultValue */);
      if (path != null && writer != null) {
        writer.writeSample(sample);
        numWriteTasksCompleted.incrementAndGet();
        return null /* success! */;
      } else {
        throw new IllegalArgumentException(
            String.format("Invalid path -- sample: %s, path: %s, writerExists: %b", sample, path,
                writer != null)); // null-safe
      }
    }

    /**
     * Block until the task is scheduled for later execution, then return it.
     * 
     * @return a progress indicator for task execution
     */
    Future<WriteTask> awaitScheduling() {
      while (scheduled.get() == null) {
        try {
          Thread.sleep(pollDelay.toMillis());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return scheduled.get();
    }

    /**
     * A hook called by the write scheduler to notify the task that it has been scheduled.
     * 
     * @param taskFuture progress indicator of task execution
     */
    void scheduledHook(Future<WriteTask> taskFuture) {
      boolean itWorked = scheduled.compareAndSet(null, taskFuture);
      if (!itWorked) {
        throw new IllegalStateException("task scheduled more than once");
      }
      numWriteTasksScheduled.incrementAndGet();
    }
  }

  private static final Duration DEFAULT_MAX_WAIT = Duration.ofSeconds(3);
  private static final Logger LOG = getLogger(AsynchronousWriter.class);
  private static final Duration PRINT_INTERVAL = Duration.ofSeconds(15);
  private static final Duration DEFAULT_POLL_DELAY = Duration.ofMillis(50);

  final Queue<WriteTask> bufferedWrites;
  final int bufferSize;
  final ExecutorService executor;
  final Duration maxWaitDuration;
  final Map<Integer, String> partitionNumToPath;
  final Map<String, AggregateSampleWriter> pathToWriter;
  final Duration pollDelay;
  final WriteScheduler scheduler = new WriteScheduler();
  final List<AggregateSampleWriter> writers;

  final Lock bufferLock = new ReentrantLock();
  final Condition bufferHasRoom = bufferLock.newCondition();
  final Condition bufferHasTask = bufferLock.newCondition();
  final AtomicBoolean isClosed = new AtomicBoolean(false);
  final AtomicInteger numWriteTasksCompleted = new AtomicInteger(0);
  final AtomicInteger numWriteTasksScheduled = new AtomicInteger(0);

  /**
   * Canonical constructor.
   * 
   * @param threadPoolSize number of threads for this writer
   * @param partitionNoToPath map from partition number to file system path
   */
  public AsynchronousWriter(int threadPoolSize, Map<Integer, String> partitionNoToPath) {
    if (threadPoolSize <= 0) {
      throw new IllegalArgumentException("threadPoolSize must be positive");
    }
    if (partitionNoToPath == null || partitionNoToPath.isEmpty()) {
      throw new IllegalArgumentException("partitionNoToPath cannot be empty");
    }

    LOG.info(String.format("initializing thread pool -- threadPoolSize: %d", threadPoolSize));
    this.executor = Executors.newFixedThreadPool(threadPoolSize);
    LOG.info("thread pool initialzied");

    this.partitionNumToPath = partitionNoToPath;
    this.writers = Lists.newArrayList();
    this.pathToWriter = Maps.newHashMap();
    this.bufferedWrites = new ArrayDeque<>();
    this.bufferSize = threadPoolSize;
    this.maxWaitDuration = DEFAULT_MAX_WAIT;
    this.pollDelay = DEFAULT_POLL_DELAY;

    // open files and track association between partition number and output file
    for (String path : partitionNoToPath.values()) {
      File file = Paths.get(path).toFile();
      if (!pathToWriter.containsKey(path)) {
        AggregateSampleWriter writer = AggregateSampleWriter.fromFile(file);
        if (writer != null) {
          writers.add(writer);
          pathToWriter.put(path, writer);
        } else {
          throw new IllegalStateException("Unable to open file -- path: " + path);
        }
      } else {
        throw new IllegalArgumentException(
            "Cannot specify identical path more than once -- path: " + path);
      }
    }

    // scheduler starts at construction time
    scheduler.start();
  }

  /**
   * Injection constructor for unit tests.
   * 
   * @param executor executor service to inject
   * @param partitionNumToPath maps partition numbers to paths of files
   * @param pathToWriter maps paths of files to writers
   * @param writers writers to inject
   * @param bufferedWrites task queue to inject
   * @param maxWaitDuration maximum time we are willing to wait for thread termination
   * @param bufferSize max. size of the queue
   */
  AsynchronousWriter(ExecutorService executor, //
      Map<Integer, String> partitionNumToPath, //
      Map<String, AggregateSampleWriter> pathToWriter, //
      List<AggregateSampleWriter> writers, //
      Queue<WriteTask> bufferedWrites, //
      Duration maxWaitDuration, //
      Duration pollDelay, //
      int bufferSize) {

    this.executor = executor;
    this.partitionNumToPath = partitionNumToPath;
    this.pathToWriter = pathToWriter;
    this.writers = writers;
    this.bufferedWrites = bufferedWrites;
    this.maxWaitDuration = maxWaitDuration;
    this.pollDelay = pollDelay;
    this.bufferSize = bufferSize;

    // scheduler starts at construction time
    scheduler.start();
  }

  @Override
  public void close() {
    if (isClosed.compareAndSet(false, true)) {
      // close requested for the first time
      for (AggregateSampleWriter asw : pathToWriter.values()) {
        asw.close();
      }

      executor.shutdown();
      Supplier<Boolean> notTerminated = () -> !executor.isTerminated();
      Duration executorWaitDuration = bestEffortWait(maxWaitDuration, notTerminated);
      if (notTerminated.get()) {
        LOG.warn(
            String.format("Could not shut down executor service within %s", executorWaitDuration));
      } else {
        LOG.info(
            String.format("Shut down executor service successfully in %s", executorWaitDuration));
      }

      Supplier<Boolean> isAlive = () -> scheduler.isAlive();
      Duration schedulerWaitDuration = bestEffortWait(maxWaitDuration, isAlive);
      if (isAlive.get()) {
        LOG.warn(String.format("Could not shut down scheduler within %s", schedulerWaitDuration));
      } else {
        LOG.info(String.format("Shut down scheduler successfully in %s", schedulerWaitDuration));
      }
    }
  }

  /**
   * Add the aggregated sample to the write queue.
   * 
   * @param sample aggregation to write
   * @return a progress indicator for the write
   */
  public Future<WriteTask> writeSample(AggregateSample sample) {
    bufferLock.lock();
    WriteTask task = null;
    try {
      while (bufferedWrites.size() == bufferSize) {
        try {
          bufferHasRoom.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      task = new WriteTask(sample);
      bufferedWrites.add(task);
      bufferHasTask.signal();
    } finally {
      bufferLock.unlock();
    }

    return task.awaitScheduling();
  }

  /**
   * Suspend this thread on the specified condition until it returns <code>false</code> or the
   * timeout expires, whichever happens first.
   * 
   * @param maxWaitDuration maximum time we are willing to wait
   * @param waitCondition a function defining the condition to be waited on
   * @return amount of time we waited
   */
  private Duration bestEffortWait(Duration maxWaitDuration, Supplier<Boolean> waitCondition) {

    Instant startWaitTime = Instant.now();
    Instant endWaitTime = startWaitTime.plus(maxWaitDuration);
    while (Instant.now().isBefore(endWaitTime) && waitCondition.get()) {
      try {
        long waitTimeInMillis = Duration.between(Instant.now(), endWaitTime).toMillis() / 2;
        executor.awaitTermination(waitTimeInMillis, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    Duration actualWaitDuration = Duration.between(startWaitTime, Instant.now());
    if (actualWaitDuration.compareTo(maxWaitDuration) > 0) {
      return maxWaitDuration;
    } else {
      return actualWaitDuration;
    }
  }
}
