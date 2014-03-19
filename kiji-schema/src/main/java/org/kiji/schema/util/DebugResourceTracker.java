/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kiji.schema.util;

import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.util.WeakIdentityHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.InternalKijiError;

/**
 * Tracks resources which should be cleaned up before JVM shutdown.
 *
 * <p>
 *   The behavior of this tracker can be controlled using the system property
 *   "org.kiji.schema.util.DebugResourceTracker.tracking_level" which may be set to any value of
 *   enum {@link DebugResourceTracker.TrackingLevel}. Tracking levels include:
 *   <ul>
 *     <li>NONE - No tracking.</li>
 *     <li>
 *       COUNTER - A count of the open resources is maintained globally. If the count is not 0 at
 *           shutdown, logs an error.
 *     </li>
 *     <li>
 *       REFERENCES - All open resources are individually tracked. Logs a debug message for each
 *           open resource at shutdown. Also enables features of COUNTER.
 *     </li>
 *   </ul>
 * </p>
 *
 * <p>
 *   This may create duplicate log messages in the cleanup log because objects may be finalized
 *   after the shutdown hook has run. This class exists to ensure that the cleanup log contains
 *   <i>at least</i> one debug message for each unclosed resource.
 * </p>
 *
 * <p>
 *   Resources which should be closed before shutdown may register themselves with this tracker by
 *   calling {@link #registerResource(Object, String)} and unregister by calling
 *   {@link #unregisterResource(Object)}. If any resources are tracked at JVM shutdown, this tracker
 *   will print log messages based on the value of {@link #TRACKING_LEVEL}.
 * </p>
 */
@ApiAudience.Framework
@ApiStability.Experimental
public final class DebugResourceTracker {
  /**
   * The system property to set to configure the tracking level of this tracker.
   */
  public static final String TRACKING_LEVEL_PROPERTY =
      "org.kiji.schema.util.DebugResourceTracker.tracking_level";

  /**
   * The configured tracking level of this tracker. This value is set by the system property
   * specified by TRACKING_LEVEL_PROPERTY, and may be set to any value of the enum
   * {@link DebugResourceTracker.TrackingLevel}.
   */
  public static final TrackingLevel TRACKING_LEVEL = TrackingLevel.valueOf(
      System.getProperty(TRACKING_LEVEL_PROPERTY, "COUNTER"));
  private static final Logger LOG = LoggerFactory.getLogger(DebugResourceTracker.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + DebugResourceTracker.class.getName());
  private static final DebugResourceTracker SINGLETON = new DebugResourceTracker();

  /**
   * Get the singleton DebugResourceTracker.
   *
   * @return the singleton DebugResourceTracker.
   */
  public static DebugResourceTracker get() {
    return SINGLETON;
  }

  /**
   * Options for tracking granularity.
   * <ul>
   *   <li>NONE - No tracking.</li>
   *   <li>
   *     COUNTER - A count of the open resources is maintained globally. If the count is not 0 at
   *         shutdown, logs an error.
   *   </li>
   *   <li>
   *     REFERENCES - All open resources are individually tracked. Logs a debug message for each
   *         open resource at shutdown. Also enables features of COUNTER.
   *   </li>
   * </ul>
   */
  public enum TrackingLevel {
    NONE, COUNTER, REFERENCES
  }

  /**
   * A synchronized wrapper around a WeakIdentityHashMap for tracking resources while allowing them
   * to be garbage collected normally.
   */
  private final Map<Object, String> mResources;
  private final AtomicInteger mCounter;

  /** Initializes the singleton DebugResourceTracker based on the value of TRACKING_LEVEL. */
  private DebugResourceTracker() {
    switch(TRACKING_LEVEL) {
      case NONE: {
        mResources = null;
        mCounter = null;
        break;
      }
      case COUNTER: {
        mResources = null;
        mCounter = new AtomicInteger(0);
        LOG.debug("Registering hook to log number of unclosed resources at shutdown.");
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        break;
      }
      case REFERENCES: {
        mResources = Collections.synchronizedMap(new WeakIdentityHashMap<Object, String>());
        mCounter = new AtomicInteger(0);
        LOG.debug("Registering hook to log details of unclosed resources at shutdown.");
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        break;
      }
      default: throw new InternalKijiError(String.format(
          "Unknown DebugResourceTracker.TrackingType: %s", TRACKING_LEVEL));
    }
  }

  /** Runs at JVM shutdown and logs warnings for open resources. */
  private final class ShutdownHook extends Thread {
    /** {@inheritDoc} */
    @Override
    public void run() {
      switch(TRACKING_LEVEL) {
        case NONE: break;
        case COUNTER: {
          logCounter();
          break;
        }
        case REFERENCES: {
          logCounterAndReferences();
          break;
        }
        default: throw new InternalKijiError(String.format(
            "Unknown DebugResourceTracker.TrackingType: %s", TRACKING_LEVEL));
      }
    }
  }

  /** Logs the number of outstanding resources during the shutdown hook. */
  private void logCounter() {
    final int unclosed = mCounter.get();
    if (0 != unclosed) {
      CLEANUP_LOG.error(
          "Found {} unclosed resources. Run with system property {}=REFERENCES for more details.",
          unclosed,
          TRACKING_LEVEL_PROPERTY);
      LOG.error(
          "Found {} unclosed resources. Run with system property {}=REFERENCES for more details.",
          unclosed,
          TRACKING_LEVEL_PROPERTY);
    } else {
      LOG.debug("JVM shutdown with no unclosed resources.");
    }
  }

  /** Logs outstanding resources during the shutdown hook. */
  private void logCounterAndReferences() {
    synchronized (mResources) {
      final int unclosed = mCounter.get();
      if (0 != unclosed) {
        CLEANUP_LOG.error("Found {} unclosed resources.", unclosed);
        LOG.error("Found {} unclosed resources.", unclosed);
      }
      while (!mResources.isEmpty()) {
        final Iterator<Map.Entry<Object, String>> iterator = mResources.entrySet().iterator();
        try {
          while (iterator.hasNext()) {
            final Map.Entry<Object, String> next = iterator.next();
            if (null != next.getValue()) {
              CLEANUP_LOG.error(
                  "{} not cleaned up before JVM shutdown: {}",
                  next.getKey(),
                  next.getValue());
            } else {
              CLEANUP_LOG.error("{} not cleaned up before JVM shutdown.", next.getKey());
            }
            iterator.remove();
          }
        } catch (ConcurrentModificationException cme) {
          // This exception indicates that a resource was garbage collected while we were iterating
          // This is normal.
          continue;
        }
      }
    }
  }

  /**
   * Registers a resource that should be cleaned up and removed before JVM shutdown.
   *
   * @param resource Object which should be cleaned up before JVM shutdown.
   * @param errorMessage error string which will be logged with the resource.toString() if the
   *     resource is not closed before shutdown. A good example of this error message is the stack
   *     trace at the time the object was created.
   */
  public void registerResource(
      final Object resource,
      final String errorMessage
  ) {
    switch(TRACKING_LEVEL) {
      case NONE: break;
      case COUNTER: {
        mCounter.incrementAndGet();
        break;
      }
      case REFERENCES: {
        mCounter.incrementAndGet();
        mResources.put(resource, errorMessage);
        break;
      }
      default: throw new InternalKijiError(String.format(
          "Unknown DebugResourceTracker.TrackingType: %s", TRACKING_LEVEL));
    }
  }

  /**
   * Removes tracking from a resource, indicating that it has been successfully cleaned up.
   *
   * @param resource Object which has been cleaned up and need no longer be tracked.
   */
  public void unregisterResource(
      final Object resource
  ) {
    switch(TRACKING_LEVEL) {
      case NONE: break;
      case COUNTER: {
        mCounter.decrementAndGet();
        break;
      }
      case REFERENCES: {
        mCounter.decrementAndGet();
        mResources.remove(resource);
        break;
      }
      default: throw new InternalKijiError(String.format(
          "Unknown DebugResourceTracker.TrackingType: %s", TRACKING_LEVEL));
    }
  }
}
