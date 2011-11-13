/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.lifecycle;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LifecycleSupervisor implements LifecycleAware {

  private static final Logger logger = LoggerFactory
      .getLogger(LifecycleSupervisor.class);

  private Map<LifecycleAware, Supervisoree> supervisedProcesses;
  private ScheduledExecutorService monitorService;

  private LifecycleState lifecycleState;

  public LifecycleSupervisor() {
    lifecycleState = LifecycleState.IDLE;
    supervisedProcesses = new HashMap<LifecycleAware, Supervisoree>();
    monitorService = Executors.newScheduledThreadPool(
        5,
        new ThreadFactoryBuilder().setNameFormat(
            "lifecycleSupervisor-" + Thread.currentThread().getId() + "-%d")
            .build());
  }

  @Override
  public synchronized void start() {

    logger.info("Starting lifecycle supervisor {}", Thread.currentThread()
        .getId());

    for (Entry<LifecycleAware, Supervisoree> entry : supervisedProcesses
        .entrySet()) {

      MonitorRunnable monitorCheckRunnable = new MonitorRunnable();

      monitorCheckRunnable.lifecycleAware = entry.getKey();
      monitorCheckRunnable.supervisoree = entry.getValue();

      monitorService.scheduleAtFixedRate(monitorCheckRunnable, 0, 3,
          TimeUnit.SECONDS);
    }

    lifecycleState = LifecycleState.START;

    logger.debug("Lifecycle supervisor started");
  }

  @Override
  public synchronized void stop() {

    logger.info("Stopping lifecycle supervisor {}", Thread.currentThread()
        .getId());

    if (monitorService != null) {
      monitorService.shutdown();

      while (!monitorService.isTerminated()) {
        try {
          monitorService.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          logger.debug("Interrupted while waiting for monitor service to stop");
          monitorService.shutdownNow();
        }
      }
    }

    for (final Entry<LifecycleAware, Supervisoree> entry : supervisedProcesses
        .entrySet()) {

      if (entry.getKey().getLifecycleState().equals(LifecycleState.START)) {
        entry.getKey().stop();
      }
    }

    /* If we've failed, preserve the error state. */
    if (lifecycleState.equals(LifecycleState.START)) {
      lifecycleState = LifecycleState.STOP;
    }

    logger.debug("Lifecycle supervisor stopped");
  }

  public synchronized void fail() {
    lifecycleState = LifecycleState.ERROR;
  }

  public synchronized void supervise(LifecycleAware lifecycleAware,
      SupervisorPolicy policy, LifecycleState desiredState) {

    Preconditions.checkState(!supervisedProcesses.containsKey(lifecycleAware),
        "Refusing to supervise " + lifecycleAware + " more than once");

    if (logger.isDebugEnabled()) {
      logger.debug("Supervising service:{} policy:{} desiredState:{}",
          new Object[] { lifecycleAware, policy, desiredState });
    }

    Supervisoree process = new Supervisoree();
    process.status = new Status();

    process.policy = policy;
    process.status.desiredState = desiredState;

    MonitorRunnable monitorRunnable = new MonitorRunnable();
    monitorRunnable.lifecycleAware = lifecycleAware;
    monitorRunnable.supervisoree = process;
    monitorRunnable.monitorService = monitorService;

    supervisedProcesses.put(lifecycleAware, process);
    monitorService.schedule(monitorRunnable, 0, TimeUnit.SECONDS);
  }

  public synchronized void unsupervise(LifecycleAware lifecycleAware) {

    Preconditions.checkState(supervisedProcesses.containsKey(lifecycleAware),
        "Unaware of " + lifecycleAware + " - can not unsupervise");

    logger.debug("Unsupervising service:{}", lifecycleAware);

    Supervisoree supervisoree = supervisedProcesses.get(lifecycleAware);
    supervisoree.status.discard = true;
  }

  public synchronized void setDesiredState(LifecycleAware lifecycleAware,
      LifecycleState desiredState) {

    Preconditions.checkState(supervisedProcesses.containsKey(lifecycleAware),
        "Unaware of " + lifecycleAware + " - can not set desired state to "
            + desiredState);

    logger.debug("Setting desiredState:{} on service:{}", desiredState,
        lifecycleAware);

    Supervisoree supervisoree = supervisedProcesses.get(lifecycleAware);
    supervisoree.status.desiredState = desiredState;
  }

  @Override
  public synchronized LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  public static class MonitorRunnable implements Runnable {

    public ScheduledExecutorService monitorService;
    public LifecycleAware lifecycleAware;
    public Supervisoree supervisoree;

    @Override
    public void run() {
      logger.debug("checking process:{} supervisoree:{}", lifecycleAware,
          supervisoree);

      long now = System.currentTimeMillis();

      if (supervisoree.status.firstSeen == null) {
        logger.debug("first time seeing {}", lifecycleAware);

        supervisoree.status.firstSeen = now;
      }

      supervisoree.status.lastSeen = now;
      supervisoree.status.lastSeenState = lifecycleAware.getLifecycleState();

      if (!lifecycleAware.getLifecycleState().equals(
          supervisoree.status.desiredState)) {

        logger
            .debug("Want to transition {} from {} to {} (failures:{})",
                new Object[] { lifecycleAware,
                    supervisoree.status.lastSeenState,
                    supervisoree.status.desiredState,
                    supervisoree.status.failures });

        switch (supervisoree.status.desiredState) {
        case START:
          try {
            lifecycleAware.start();
          } catch (Exception e) {
            logger.error("Unable to start " + lifecycleAware
                + " - Exception follows.", e);
            supervisoree.status.failures++;
          }
          break;
        case STOP:
          try {
            lifecycleAware.stop();
          } catch (Exception e) {
            logger.error("Unable to stop " + lifecycleAware
                + " - Exception follows.", e);
            supervisoree.status.failures++;
          }
          break;
        default:
          logger.warn("I refuse to acknowledge {} as a desired state",
              supervisoree.status.desiredState);
        }

        if (!supervisoree.policy.isValid(lifecycleAware, supervisoree.status)) {
          logger.error(
              "Policy {} of {} has been violated - supervisor should exit!",
              supervisoree.policy, lifecycleAware);
        }
      }

      if (!supervisoree.status.discard) {
        monitorService.schedule(this, 3, TimeUnit.SECONDS);
      } else {
        logger.debug("Halting monitoring on {}", supervisoree);
      }

      logger.debug("Status check complete");
    }

  }

  public static class Status {
    public Long firstSeen;
    public Long lastSeen;
    public LifecycleState lastSeenState;
    public LifecycleState desiredState;
    public int failures;
    public boolean discard;

    @Override
    public String toString() {
      return "{ lastSeen:" + lastSeen + " lastSeenState:" + lastSeenState
          + " desiredState:" + desiredState + " firstSeen:" + firstSeen
          + " failures:" + failures + " discard:" + discard + " }";
    }

  }

  public static abstract class SupervisorPolicy {

    abstract boolean isValid(LifecycleAware object, Status status);

    public static class AlwaysRestartPolicy extends SupervisorPolicy {

      @Override
      boolean isValid(LifecycleAware object, Status status) {
        return true;
      }
    }

    public static class OnceOnlyPolicy extends SupervisorPolicy {

      @Override
      boolean isValid(LifecycleAware object, Status status) {
        return status.failures == 0;
      }
    }

  }

  private static class Supervisoree {

    public SupervisorPolicy policy;
    public Status status;

    @Override
    public String toString() {
      return "{ status:" + status + " policy:" + policy + " }";
    }

  }

}
