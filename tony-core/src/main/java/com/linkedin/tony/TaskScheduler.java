/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.tony.models.JobContainerRequest;
import com.linkedin.tony.util.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.linkedin.tony.TonySession.TonyTask;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import static com.linkedin.tony.TonyConfigurationKeys.APPLICATION_PLACEMENT_SPEC;

public class TaskScheduler {
  private static final Log LOG = LogFactory.getLog(TaskScheduler.class);
  private TonySession session;
  private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;
  private FileSystem resourceFs;
  private Configuration tonyConf;

  // job with dependency -> (dependent job name, number of instances for that job)
  private Map<JobContainerRequest, Map<String, Integer>> taskDependencyMap = new HashMap<>();
  private Map<String, LocalResource> localResources;
  private Map<String, Map<String, LocalResource>> jobTypeToContainerResources;

  //=== Placement constraint retry support ===
  private static class PlacementTracker {
    final JobContainerRequest originalRequest;
    int expectedCount;
    List<Long> outstandingAllocationIds;
    long lastIssueTs;

    PlacementTracker(JobContainerRequest originalRequest, List<Long> outstandingAllocationIds) {
      this.originalRequest = originalRequest;
      this.expectedCount = originalRequest.getNumInstances();
      this.outstandingAllocationIds = outstandingAllocationIds;
      this.lastIssueTs = System.currentTimeMillis();
    }
  }

  private final Map<String, PlacementTracker> placementTrackers = new ConcurrentHashMap<>();
  private java.util.concurrent.ScheduledExecutorService placementRetryExecutor;
  private int placementRetryIntervalMs;

  boolean dependencyCheckPassed = true;

  public TaskScheduler(TonySession session, AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient, Map<String, LocalResource> localResources,
      FileSystem resourceFs, Configuration tonyConf, Map<String, Map<String, LocalResource>> jobTypeToContainerResources) {
    this.session = session;
    this.amRMClient = amRMClient;
    this.localResources = localResources;
    this.resourceFs = resourceFs;
    this.tonyConf = tonyConf;
    this.jobTypeToContainerResources = jobTypeToContainerResources;

    this.placementRetryIntervalMs = tonyConf.getInt(
        TonyConfigurationKeys.APPLICATION_PLACEMENT_RETRY_INTERVAL_MS,
        TonyConfigurationKeys.DEFAULT_APPLICATION_PLACEMENT_RETRY_INTERVAL_MS);

    if (placementRetryIntervalMs > 0) {
      this.placementRetryExecutor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "placement-retry-thread");
        t.setDaemon(true);
        return t;
      });
      this.placementRetryExecutor.scheduleAtFixedRate(this::retryUnallocatedPlacementRequests,
          placementRetryIntervalMs,
          placementRetryIntervalMs,
          java.util.concurrent.TimeUnit.MILLISECONDS);
    }
  }

  public void scheduleTasks() {
    final List<JobContainerRequest> requests = session.getContainersRequests();

    if (!isDAG(requests)) {
      LOG.error("TonY execution graph does not form a DAG, exiting.");
      session.setFinalStatus(FinalApplicationStatus.FAILED, "App failed due to it not being a DAG.");
      dependencyCheckPassed = false;
      return;
    }

    buildTaskDependencyGraph(requests);

    // start/schedule jobs that have no dependency requirements
    for (JobContainerRequest request : requests) {
      if (checkDependencySatisfied(request)) {
        scheduleJob(request);
      }
    }
  }

  private void buildTaskDependencyGraph(List<JobContainerRequest> requests) {
    for (JobContainerRequest request : requests) {
      for (String dependsOn : request.getDependsOn()) {
        if (!dependsOn.isEmpty()) {
          taskDependencyMap.putIfAbsent(request, new HashMap<>());
          Map<String, Integer> dependenciesForTask = taskDependencyMap.get(request);
          dependenciesForTask.put(dependsOn, session.getContainerRequestForType(dependsOn).getNumInstances());
          taskDependencyMap.put(request, dependenciesForTask);
        }
      }
    }
  }

  @VisibleForTesting
  boolean checkDependencySatisfied(JobContainerRequest request) {
    return taskDependencyMap.get(request) == null || taskDependencyMap.get(request).isEmpty();
  }

  private void scheduleJob(JobContainerRequest request) {
    if (request.getPlacementSpec() != null || StringUtils.isNotEmpty(tonyConf.get(APPLICATION_PLACEMENT_SPEC))) {
      // this should use newer api of Yarn with this placement constraint feature,
      // only be supported in hadoop 3.2.x
      //
      // Tips: the app level placement constraint spec must be together with scheduling
      //       request api, otherwise, it is invalid.
      List<Long> allocIds = HadoopCompatibleAdapter.constructAndAddSchedulingRequest(amRMClient, request);
      // track for retry
      placementTrackers.put(request.getJobName(), new PlacementTracker(request, allocIds));
    } else {
      AMRMClient.ContainerRequest containerAsk = Utils.setupContainerRequestForRM(request);
      for (int i = 0; i < request.getNumInstances(); i++) {
        amRMClient.addContainerRequest(containerAsk);
      }
    }

    String jobName = request.getJobName();
    jobTypeToContainerResources.putIfAbsent(jobName, getContainerResources(jobName));

    session.addNumExpectedTask(request.getNumInstances());
  }

  private Map<String, LocalResource> getContainerResources(String jobName) {
    Map<String, LocalResource> containerResources = new ConcurrentHashMap<>(localResources);
    String[] resources = tonyConf.getStrings(TonyConfigurationKeys.getResourcesKey(jobName));
    Utils.addResources(resources, containerResources, tonyConf);

    // All resources available to all containers
    resources = tonyConf.getStrings(TonyConfigurationKeys.getContainerResourcesKey());
    Utils.addResources(resources, containerResources, tonyConf);
    return containerResources;
  }

  synchronized void registerDependencyCompleted(String jobName) {
    taskDependencyMap.forEach((k, v) -> {
      if (v.containsKey(jobName)) {
        int numContainersLeft = v.get(jobName);
        numContainersLeft--;

        if (numContainersLeft == 0) {
          v.remove(jobName);
        } else {
          v.put(jobName, numContainersLeft);
        }
      }
    });

    Iterator<JobContainerRequest> waitingRequestItr = taskDependencyMap.keySet().iterator();
    while (waitingRequestItr.hasNext()) {
      JobContainerRequest waitingRequest = waitingRequestItr.next();
      if (checkDependencySatisfied((waitingRequest))) {
        waitingRequestItr.remove();
        scheduleJob(waitingRequest);
      }
    }
  }

  /**
   * Periodically invoked to retry placement-constrained scheduling requests that have not yet been satisfied.
   */
  private void retryUnallocatedPlacementRequests() {
    if (placementTrackers.isEmpty()) {
      return;
    }

    long now = System.currentTimeMillis();

    placementTrackers.entrySet().removeIf(entry -> {
      String jobName = entry.getKey();
      PlacementTracker tracker = entry.getValue();

      // Compute number of allocated tasks for this job.
      int allocated = 0;
      Map<String, TonyTask[]> tasksMap = session.getTonyTasks();
      TonyTask[] tasksArr = tasksMap.get(jobName);
      if (tasksArr != null) {
        for (TonyTask t : tasksArr) {
          if (t != null) {
            allocated++;
          }
        }
      }

      int remaining = tracker.expectedCount - allocated;

      if (remaining <= 0) {
        // All containers have been allocated – no more tracking required.
        HadoopCompatibleAdapter.removeSchedulingRequests(amRMClient, tracker.outstandingAllocationIds);
        return true; // remove entry
      }

      // Not all allocated – check if it's time to retry.
      if (now - tracker.lastIssueTs >= placementRetryIntervalMs) {
        // Cancel previous outstanding scheduling requests.
        HadoopCompatibleAdapter.removeSchedulingRequests(amRMClient, tracker.outstandingAllocationIds);

        // Issue new requests for remaining containers.
        JobContainerRequest orig = tracker.originalRequest;
        JobContainerRequest newReq = new JobContainerRequest(orig.getJobName(), remaining, orig.getMemory(),
            orig.getVCores(), orig.getGPU(), orig.getPriority(), orig.getNodeLabelsExpression(), orig.getDependsOn(),
            orig.getPlacementSpec(), orig.getAllocationTags());

        List<Long> newIds = HadoopCompatibleAdapter.constructAndAddSchedulingRequest(amRMClient, newReq);

        tracker.outstandingAllocationIds = newIds;
        tracker.lastIssueTs = now;
      }

      return false; // keep tracking
    });
  }

  static boolean isDAG(final List<JobContainerRequest> containersRequests) {
    Set<JobContainerRequest> visited = new HashSet<>();

    for (JobContainerRequest containerRequest : containersRequests) {
      if (!visited.contains(containerRequest) && !isSubgraphDAG(containerRequest, new ArrayList<>(), containersRequests, visited)) {
        return false;
      }
    }

    return true;
  }

  static boolean isSubgraphDAG(JobContainerRequest node, List<JobContainerRequest> pathTrace,
      final List<JobContainerRequest> containerRequests, Set<JobContainerRequest> visited) {
    if (pathTrace.contains(node)) {
      return false;
    }
    if (visited.contains(node)) {
      return true;
    }

    pathTrace.add(node);
    visited.add(node);

    List<JobContainerRequest> dependencies = containerRequests.stream()
        .filter(x -> node.getDependsOn().contains(x.getJobName()))
        .collect(Collectors.toList());
    for (JobContainerRequest dependency : dependencies) {
      if (!isSubgraphDAG(dependency, pathTrace, containerRequests, visited)) {
        return false;
      }
    }

    pathTrace.remove(node);

    return true;
  }
}
