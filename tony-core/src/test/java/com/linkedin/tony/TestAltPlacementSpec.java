/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
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
 * Verifies the alternative placement spec fallback logic implemented in {@link TaskScheduler}.
 */
public class TestAltPlacementSpec {
  private TonySession session;
  private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;
  private Map<String, Map<String, LocalResource>> jobTypeToContainerResources;
  private Map<String, LocalResource> localResources;
  private Configuration conf;
  private FileSystem fs;
  private AtomicInteger addCounter;

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

  /**
   * Test fallback to alternative placement spec after timeout.
   */
  @Test(timeOut = 5000)
  public void testFallbackToAltPlacementSpecAfterTimeout() throws InterruptedException {
    // Configure primary and alternative placement specs
    when(conf.get(TonyConfigurationKeys.APPLICATION_PLACEMENT_SPEC)).thenReturn("IN,node,worker");
    when(conf.get(TonyConfigurationKeys.APPLICATION_ALT_PLACEMENT_SPEC)).thenReturn("NOTIN,node,worker");
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_TIMEOUT_MS), anyInt()))
        .thenReturn(200); // 200ms timeout for quick test
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_ATTEMPTS), anyInt()))
        .thenReturn(10); // High attempts so timeout triggers first

    // Build placement-constrained request (3 containers)
    JobContainerRequest workerReq = new JobContainerRequest("worker", 3, 2048, 1, 0, 1,
        "", Collections.emptyList(), "IN,node,worker", Collections.singletonList("tagA"));
    List<JobContainerRequest> requests = Collections.singletonList(workerReq);

    when(session.getContainersRequests()).thenReturn(requests);
    when(session.getContainerRequestForType("worker")).thenReturn(workerReq);
    when(session.getTonyTasks()).thenReturn(Collections.emptyMap()); // no allocation

    TaskScheduler scheduler = new TaskScheduler(session, amRMClient, localResources, fs, conf, jobTypeToContainerResources);
    scheduler.scheduleTasks();

    // Wait for timeout to trigger alternative placement spec
    Thread.sleep(800);

    // Should have at least initial submission + fallback submission
    assertTrue(addCounter.get() >= 2, "Expected at least two addSchedulingRequests invocations for fallback, got " + addCounter.get());
  }

  /**
   * Test fallback to alternative placement spec after max attempts.
   */
  @Test(timeOut = 5000)
  public void testFallbackToAltPlacementSpecAfterMaxAttempts() throws InterruptedException {
    // Configure primary and alternative placement specs
    when(conf.get(TonyConfigurationKeys.APPLICATION_PLACEMENT_SPEC)).thenReturn("IN,node,worker");
    when(conf.get(TonyConfigurationKeys.APPLICATION_ALT_PLACEMENT_SPEC)).thenReturn("NOTIN,node,worker");
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_TIMEOUT_MS), anyInt()))
        .thenReturn(10000); // Long timeout so attempts trigger first
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_ATTEMPTS), anyInt()))
        .thenReturn(2); // Only 2 attempts before fallback

    // Build placement-constrained request (3 containers)
    JobContainerRequest workerReq = new JobContainerRequest("worker", 3, 2048, 1, 0, 1,
        "", Collections.emptyList(), "IN,node,worker", Collections.singletonList("tagA"));
    List<JobContainerRequest> requests = Collections.singletonList(workerReq);

    when(session.getContainersRequests()).thenReturn(requests);
    when(session.getContainerRequestForType("worker")).thenReturn(workerReq);
    when(session.getTonyTasks()).thenReturn(Collections.emptyMap()); // no allocation

    TaskScheduler scheduler = new TaskScheduler(session, amRMClient, localResources, fs, conf, jobTypeToContainerResources);
    scheduler.scheduleTasks();

    // Wait for attempts to trigger alternative placement spec
    Thread.sleep(800);

    // Should have at least initial submission + 2 retries + fallback submission
    assertTrue(addCounter.get() >= 4, "Expected at least four addSchedulingRequests invocations for attempts fallback, got " + addCounter.get());
  }

  /**
   * Test no fallback when containers are allocated before timeout/attempts.
   */
  @Test(timeOut = 3000)
  public void testNoFallbackWhenContainersAllocated() throws InterruptedException {
    // Configure primary and alternative placement specs
    when(conf.get(TonyConfigurationKeys.APPLICATION_PLACEMENT_SPEC)).thenReturn("IN,node,worker");
    when(conf.get(TonyConfigurationKeys.APPLICATION_ALT_PLACEMENT_SPEC)).thenReturn("NOTIN,node,worker");
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_TIMEOUT_MS), anyInt()))
        .thenReturn(200); // 200ms timeout
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_ATTEMPTS), anyInt()))
        .thenReturn(2); // 2 attempts

    // Build placement-constrained request (2 containers)
    JobContainerRequest workerReq = new JobContainerRequest("worker", 2, 2048, 1, 0, 1,
        "", Collections.emptyList(), "IN,node,worker", Collections.singletonList("tagA"));
    List<JobContainerRequest> requests = Collections.singletonList(workerReq);

    when(session.getContainersRequests()).thenReturn(requests);
    when(session.getContainerRequestForType("worker")).thenReturn(workerReq);

    // Dynamic task map that test can mutate - simulate all containers allocated
    AtomicReference<Map<String, TonySession.TonyTask[]>> taskMapRef = new AtomicReference<>(Collections.emptyMap());
    when(session.getTonyTasks()).thenAnswer(inv -> taskMapRef.get());

    TaskScheduler scheduler = new TaskScheduler(session, amRMClient, localResources, fs, conf, jobTypeToContainerResources);
    scheduler.scheduleTasks(); // first submission

    // Simulate all containers become allocated before timeout
    TonySession.TonyTask mockTask1 = mock(TonySession.TonyTask.class);
    TonySession.TonyTask mockTask2 = mock(TonySession.TonyTask.class);
    TonySession.TonyTask[] arr = new TonySession.TonyTask[] { mockTask1, mockTask2 };
    Map<String, TonySession.TonyTask[]> newMap = Collections.singletonMap("worker", arr);
    taskMapRef.set(newMap);

    // Wait beyond timeout
    Thread.sleep(600);

    // Should have only initial submission since all containers were allocated
    assertTrue(addCounter.get() == 1, "Expected only one addSchedulingRequests invocation when all allocated, got " + addCounter.get());
  }

  /**
   * Test no fallback when alternative placement spec is not configured.
   */
  @Test(timeOut = 3000)
  public void testNoFallbackWhenAltPlacementSpecNotConfigured() throws InterruptedException {
    // Configure only primary placement spec
    when(conf.get(TonyConfigurationKeys.APPLICATION_PLACEMENT_SPEC)).thenReturn("IN,node,worker");
    when(conf.get(TonyConfigurationKeys.APPLICATION_ALT_PLACEMENT_SPEC)).thenReturn(null);
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_TIMEOUT_MS), anyInt()))
        .thenReturn(200); // 200ms timeout
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_ATTEMPTS), anyInt()))
        .thenReturn(2); // 2 attempts

    // Build placement-constrained request (3 containers)
    JobContainerRequest workerReq = new JobContainerRequest("worker", 3, 2048, 1, 0, 1,
        "", Collections.emptyList(), "IN,node,worker", Collections.singletonList("tagA"));
    List<JobContainerRequest> requests = Collections.singletonList(workerReq);

    when(session.getContainersRequests()).thenReturn(requests);
    when(session.getContainerRequestForType("worker")).thenReturn(workerReq);
    when(session.getTonyTasks()).thenReturn(Collections.emptyMap()); // no allocation

    TaskScheduler scheduler = new TaskScheduler(session, amRMClient, localResources, fs, conf, jobTypeToContainerResources);
    scheduler.scheduleTasks();

    // Wait beyond timeout
    Thread.sleep(600);

    // Should continue with regular retries without fallback
    assertTrue(addCounter.get() >= 2, "Expected regular retries without fallback, got " + addCounter.get());
  }

  /**
   * Test partial allocation with fallback - some containers allocated with primary spec,
   * remaining fall back to alternative spec.
   */
  @Test(timeOut = 5000)
  public void testPartialAllocationWithFallback() throws InterruptedException {
    // Configure primary and alternative placement specs
    when(conf.get(TonyConfigurationKeys.APPLICATION_PLACEMENT_SPEC)).thenReturn("IN,node,worker");
    when(conf.get(TonyConfigurationKeys.APPLICATION_ALT_PLACEMENT_SPEC)).thenReturn("NOTIN,node,worker");
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_TIMEOUT_MS), anyInt()))
        .thenReturn(200); // 200ms timeout
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_ATTEMPTS), anyInt()))
        .thenReturn(10); // High attempts so timeout triggers first

    // Build placement-constrained request (3 containers)
    JobContainerRequest workerReq = new JobContainerRequest("worker", 3, 2048, 1, 0, 1,
        "", Collections.emptyList(), "IN,node,worker", Collections.singletonList("tagA"));
    List<JobContainerRequest> requests = Collections.singletonList(workerReq);

    when(session.getContainersRequests()).thenReturn(requests);
    when(session.getContainerRequestForType("worker")).thenReturn(workerReq);

    // Dynamic task map - simulate partial allocation
    AtomicReference<Map<String, TonySession.TonyTask[]>> taskMapRef = new AtomicReference<>(Collections.emptyMap());
    when(session.getTonyTasks()).thenAnswer(inv -> taskMapRef.get());

    TaskScheduler scheduler = new TaskScheduler(session, amRMClient, localResources, fs, conf, jobTypeToContainerResources);
    scheduler.scheduleTasks(); // first submission

    // Simulate one container becomes allocated before timeout
    TonySession.TonyTask mockTask = mock(TonySession.TonyTask.class);
    TonySession.TonyTask[] arr = new TonySession.TonyTask[] { mockTask, null, null };
    Map<String, TonySession.TonyTask[]> newMap = Collections.singletonMap("worker", arr);
    taskMapRef.set(newMap);

    // Wait for timeout to trigger alternative placement spec
    Thread.sleep(600);

    // Should have initial submission + fallback for remaining containers
    assertTrue(addCounter.get() >= 2, "Expected at least two addSchedulingRequests invocations for partial allocation fallback, got " + addCounter.get());
  }

  /**
   * Test job-specific alternative placement spec fallback.
   * This test verifies that job-specific alternative placement specs are used when configured.
   */
  @Test(timeOut = 4000)
  public void testJobSpecificAltPlacementSpecFallback() throws InterruptedException {
    // Configure job-specific alternative placement spec for worker
    when(conf.get(TonyConfigurationKeys.getAltPlacementSpecKey("worker"))).thenReturn("CARDINALITY,node,worker,0,2");
    when(conf.get(TonyConfigurationKeys.APPLICATION_ALT_PLACEMENT_SPEC)).thenReturn("NOTIN,node,exclude");
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_ATTEMPTS), anyInt()))
        .thenReturn(2); // Fallback after 2 attempts

    JobContainerRequest workerReq = new JobContainerRequest("worker", 3, 2048, 1, 0, 1,
        "", Collections.emptyList(), "IN,node,worker", Collections.singletonList("worker"));
    List<JobContainerRequest> requests = Collections.singletonList(workerReq);

    when(session.getContainersRequests()).thenReturn(requests);
    when(session.getContainerRequestForType("worker")).thenReturn(workerReq);
    when(session.getTonyTasks()).thenReturn(Collections.emptyMap()); // no allocation

    TaskScheduler scheduler = new TaskScheduler(session, amRMClient, localResources, fs, conf, jobTypeToContainerResources);
    scheduler.scheduleTasks();

    // Wait for fallback to job-specific alternative placement spec
    Thread.sleep(800);

    assertTrue(addCounter.get() >= 3, "Expected initial + retries + job-specific fallback, got " + addCounter.get());
  }

  /**
   * Test fallback hierarchy: job-specific alt spec takes precedence over application-level.
   */
  @Test(timeOut = 4000)
  public void testAltPlacementSpecHierarchy() throws InterruptedException {
    // Configure BOTH job-specific and application-level alternative placement specs
    when(conf.get(TonyConfigurationKeys.getAltPlacementSpecKey("worker"))).thenReturn("CARDINALITY,node,worker,0,3");
    when(conf.get(TonyConfigurationKeys.APPLICATION_ALT_PLACEMENT_SPEC)).thenReturn("NOTIN,node,exclude");
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_ATTEMPTS), anyInt()))
        .thenReturn(2);

    JobContainerRequest workerReq = new JobContainerRequest("worker", 2, 2048, 1, 0, 1,
        "", Collections.emptyList(), "IN,node,specific", Collections.singletonList("worker"));
    List<JobContainerRequest> requests = Collections.singletonList(workerReq);

    when(session.getContainersRequests()).thenReturn(requests);
    when(session.getContainerRequestForType("worker")).thenReturn(workerReq);
    when(session.getTonyTasks()).thenReturn(Collections.emptyMap());

    TaskScheduler scheduler = new TaskScheduler(session, amRMClient, localResources, fs, conf, jobTypeToContainerResources);
    scheduler.scheduleTasks();

    Thread.sleep(800);

    // Should use job-specific alt spec (CARDINALITY,node,worker,0,3) NOT application-level (NOTIN,node,exclude)
    assertTrue(addCounter.get() >= 3, "Expected hierarchy fallback to job-specific alt spec, got " + addCounter.get());
  }

  /**
   * Test worker job with specific alternative placement spec.
   */
  @Test(timeOut = 4000)
  public void testWorkerJobWithSpecificAltSpec() throws InterruptedException {
    // Configure worker-specific alternative placement spec
    when(conf.get(TonyConfigurationKeys.getAltPlacementSpecKey("worker"))).thenReturn("CARDINALITY,node,worker,0,2");
    when(conf.get(TonyConfigurationKeys.APPLICATION_ALT_PLACEMENT_SPEC)).thenReturn("CARDINALITY,node,fallback,0,1");
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_ATTEMPTS), anyInt()))
        .thenReturn(2);

    // Create worker request
    JobContainerRequest workerReq = new JobContainerRequest("worker", 2, 2048, 1, 0, 1,
        "", Collections.emptyList(), "IN,node,worker", Collections.singletonList("worker"));
    List<JobContainerRequest> requests = Collections.singletonList(workerReq);

    when(session.getContainersRequests()).thenReturn(requests);
    when(session.getContainerRequestForType("worker")).thenReturn(workerReq);
    when(session.getTonyTasks()).thenReturn(Collections.emptyMap());

    TaskScheduler scheduler = new TaskScheduler(session, amRMClient, localResources, fs, conf, jobTypeToContainerResources);
    scheduler.scheduleTasks();

    Thread.sleep(800);

    // Should use worker-specific alternative spec after retries
    assertTrue(addCounter.get() >= 3, "Expected initial requests + retries with worker-specific alt spec, got " + addCounter.get());
  }

  /**
   * Test fallback to application-level when job-specific alternative is not configured.
   */
  @Test(timeOut = 4000)
  public void testFallbackToApplicationLevelWhenJobSpecificNotConfigured() throws InterruptedException {
    // Configure ONLY application-level alternative placement spec (no job-specific)
    when(conf.get(TonyConfigurationKeys.getAltPlacementSpecKey("worker"))).thenReturn(null);
    when(conf.get(TonyConfigurationKeys.APPLICATION_ALT_PLACEMENT_SPEC)).thenReturn("CARDINALITY,node,fallback,0,2");
    when(conf.getInt(eq(TonyConfigurationKeys.APPLICATION_PLACEMENT_ALT_FALLBACK_ATTEMPTS), anyInt()))
        .thenReturn(2);

    JobContainerRequest workerReq = new JobContainerRequest("worker", 2, 2048, 1, 0, 1,
        "", Collections.emptyList(), "IN,node,worker", Collections.singletonList("worker"));
    List<JobContainerRequest> requests = Collections.singletonList(workerReq);

    when(session.getContainersRequests()).thenReturn(requests);
    when(session.getContainerRequestForType("worker")).thenReturn(workerReq);
    when(session.getTonyTasks()).thenReturn(Collections.emptyMap());

    TaskScheduler scheduler = new TaskScheduler(session, amRMClient, localResources, fs, conf, jobTypeToContainerResources);
    scheduler.scheduleTasks();

    Thread.sleep(800);

    // Should fall back to application-level alternative spec
    assertTrue(addCounter.get() >= 3, "Expected fallback to application-level alt spec, got " + addCounter.get());
  }
} 