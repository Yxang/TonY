/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.linkedin.tony.models.JobContainerRequest;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

/**
 * Verifies the retry logic for placement-constrained container requests implemented in {@link TaskScheduler}.
 */
public class TestPlacementConstraintRetry {
  private TonySession session;
  private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;
  private Map<String, Map<String, LocalResource>> jobTypeToContainerResources;
  private Map<String, LocalResource> localResources;
  private Configuration conf;
  private FileSystem fs;

  @BeforeMethod
  public void commonSetup() {
    session = mock(TonySession.class);

    // custom mock that counts reflective invocations
    AtomicInteger addCount = new AtomicInteger();
    AtomicInteger removeCount = new AtomicInteger();
    amRMClient = mock(AMRMClientAsync.class, invocation -> {
      String m = invocation.getMethod().getName();
      if ("addSchedulingRequests".equals(m)) {
        addCount.incrementAndGet();
      } else if ("removeSchedulingRequests".equals(m)) {
        removeCount.incrementAndGet();
      }
      return null; // default stub
    });
    jobTypeToContainerResources = spy(new HashMap<>());
    localResources = spy(new HashMap<>());
    conf = mock(Configuration.class);
    fs = mock(FileSystem.class);

    // Configure retry interval to be short so test runs quickly
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_RETRY_INTERVAL_MS), anyInt()))
        .thenReturn(100);
    when(conf.getStrings(any())).thenReturn(null);

    // Store counters in mock for later assertions via lambdas
    this.addCounter = addCount;
  }

  private AtomicInteger addCounter;

  /**
   * Scenario 1 – nothing gets allocated before the first retry cycle: we should observe a cancel + re-add.
   */
  @Test(timeOut = 3000)
  public void testRetryResubmitsWhenNothingAllocated() throws InterruptedException {
    // Build placement-constrained request (3 containers)
    JobContainerRequest workerReq = new JobContainerRequest("worker", 3, 2048, 1, 0, 1,
        "", Collections.emptyList(), "java=true", Collections.singletonList("tagA"));
    List<JobContainerRequest> requests = Collections.singletonList(workerReq);

    when(session.getContainersRequests()).thenReturn(requests);
    when(session.getContainerRequestForType("worker")).thenReturn(workerReq);
    when(session.getTonyTasks()).thenReturn(Collections.emptyMap()); // no allocation

    TaskScheduler scheduler = new TaskScheduler(session, amRMClient, localResources, fs, conf, jobTypeToContainerResources);
    scheduler.scheduleTasks();

    // Wait for activity
    Thread.sleep(800);

    assertTrue(addCounter.get() >= 2, "Expected at least two addSchedulingRequests invocations, got " + addCounter.get());
    assertTrue(removeCounter.get() >= 1, "Expected at least one removeSchedulingRequests invocation");
  }

  /**
   * Scenario 2 – part of the request is allocated before retry. Expect scheduler to cancel outstanding requests and
   * re-issue only for the remaining containers.
   */
  @Test(timeOut = 4000)
  public void testRetryOnlyForUnallocatedContainers() throws InterruptedException {
    // Request 3 containers with placement constraint
    JobContainerRequest workerReq = new JobContainerRequest("worker", 3, 2048, 1, 0, 1,
        "", Collections.emptyList(), "java=true", Collections.singletonList("tagA"));
    List<JobContainerRequest> requests = Collections.singletonList(workerReq);

    when(session.getContainersRequests()).thenReturn(requests);
    when(session.getContainerRequestForType("worker")).thenReturn(workerReq);

    // Dynamic task map that test can mutate
    AtomicReference<Map<String, TonySession.TonyTask[]>> taskMapRef = new AtomicReference<>(Collections.emptyMap());
    when(session.getTonyTasks()).thenAnswer(inv -> taskMapRef.get());

    TaskScheduler scheduler = new TaskScheduler(session, amRMClient, localResources, fs, conf, jobTypeToContainerResources);
    scheduler.scheduleTasks(); // first submission

    // Simulate one container becomes allocated before retry – non-null task entry counts as allocated
    TonySession.TonyTask mockTask = mock(TonySession.TonyTask.class);
    TonySession.TonyTask[] arr = new TonySession.TonyTask[] { mockTask, null, null };
    Map<String, TonySession.TonyTask[]> newMap = Collections.singletonMap("worker", arr);
    taskMapRef.set(newMap);

    int initialAdds = addCounter.get();

    // Wait for retry cycle to fire
    Thread.sleep(500);

    assertTrue(removeCounter.get() > initialRemoves, "Expected at least one cancellation after retry");
    assertTrue(addCounter.get() > initialAdds, "Expected at least one additional addSchedulingRequests after retry");
  }
} 