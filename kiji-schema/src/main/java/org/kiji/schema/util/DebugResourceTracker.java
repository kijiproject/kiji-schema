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

import java.io.Closeable;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.exception.ExceptionUtils;
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
 *   Resources which should be closed before shutdown may register themselves with this tracker by
 *   calling {@link #registerResource} and unregister by calling {@link #unregisterResource}.
 *   If any resources are tracked at JVM shutdown, this tracker will print log messages based on the
 *   value of {@link #TRACKING_LEVEL}.
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
  public static final TrackingLevel TRACKING_LEVEL =
      TrackingLevel.valueOf(System.getProperty(TRACKING_LEVEL_PROPERTY, "COUNTER"));
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

  /** Count of tracked resources. */
  private final AtomicInteger mCounter;

  /** Tracks individual resources. */
  private final ReferenceTracker mReferenceTracker;

  /** Initializes the singleton DebugResourceTracker based on the value of TRACKING_LEVEL. */
  private DebugResourceTracker() {
    switch(TRACKING_LEVEL) {
      case NONE: {
        mReferenceTracker = null;
        mCounter = null;
        break;
      }
      case COUNTER: {
        mReferenceTracker = null;
        mCounter = new AtomicInteger(0);
        LOG.debug("Registering hook to log number of unclosed resources at shutdown.");
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        break;
      }
      case REFERENCES: {
        mReferenceTracker = new ReferenceTracker();
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
          logCounter();
          mReferenceTracker.close();
          break;
        }
        default: throw new InternalKijiError(String.format(
            "Unknown DebugResourceTracker.TrackingType: %s", TRACKING_LEVEL));
      }
    }
  }

  /** Logs the number of outstanding resources during the shutdown hook. */
  private void logCounter() {
    final String message;
    switch(TRACKING_LEVEL) {
      case COUNTER: {
        message = String.format(
            "Found {} unclosed resources. Run with system property %s=REFERENCES for more details.",
            TRACKING_LEVEL_PROPERTY);
        break;
      }
      case REFERENCES: {
        message =  "Found {} unclosed resources.";
        break;
      }
      default: throw new InternalKijiError(String.format(
          "Illegal DebugResourceTracker.TrackingType: %s", TRACKING_LEVEL));
    }

    final int count = mCounter.get();
    if (0 != count) {
      CLEANUP_LOG.error(message, count);
      LOG.error(message, count);
    } else {
      LOG.debug("JVM shutdown with no unclosed resources.");
    }
  }

  /**
   * Registers a resource that should be cleaned up and removed before JVM shutdown. When using
   * reference level tracking, the message and the current stack trace will be logged.
   *
   * @param resource Object which should be cleaned up before JVM shutdown.
   * @param message string which will be logged along with the current stack trace if the resource
   *    is not closed before shutdown. A good example of this error message is the stack trace at
   *    the time the object was created.
   */
  public void registerResource(
      final Object resource,
      final String message
  ) {
    switch(TRACKING_LEVEL) {
      case NONE: break;
      case COUNTER: {
        mCounter.incrementAndGet();
        break;
      }
      case REFERENCES: {
        // Skip two stack frames. One for the throwable, one for this method.
        final String stackTrace = Joiner.on('\n')
            .join(Iterables.skip(
                    Lists.newArrayList(ExceptionUtils.getStackFrames(new Throwable())), 2));
        mCounter.incrementAndGet();
        mReferenceTracker.registerResource(resource, message, stackTrace);
        break;
      }
      default: throw new InternalKijiError(String.format(
          "Unknown DebugResourceTracker.TrackingType: %s", TRACKING_LEVEL));
    }
  }

  /**
   * Registers a resource that should be cleaned up and removed before JVM shutdown. When using
   * reference level tracking, the message will be the {@link #toString()} of the resource, and the
   * current stack trace.
   * <p>
   * This method will call {@link #toString()} on the passed in resource, so it must be in a valid
   * state.
   *
   * @param resource Object which should be cleaned up before JVM shutdown.
   */
  public void registerResource(
      final Object resource
  ) {
    switch(TRACKING_LEVEL) {
      case NONE: break;
      case COUNTER: {
        mCounter.incrementAndGet();
        break;
      }
      case REFERENCES: {
        // Skip two stack frames. One for the throwable, one for this method.
        final String stackTrace = Joiner.on('\n')
            .join(Iterables.skip(
                Lists.newArrayList(ExceptionUtils.getStackFrames(new Throwable())), 2));

        mCounter.incrementAndGet();
        mReferenceTracker.registerResource(resource, resource.toString(), stackTrace);
        break;
      }
      default: throw new InternalKijiError(String.format(
          "Unknown DebugResourceTracker.TrackingType: %s", TRACKING_LEVEL));
    }
  }

  /**
   * Removes tracking from a resource, indicating that it has been successfully cleaned up.
   * <p>
   * This method will call {@link #toString()} on the passed in resource, so it must be in a valid
   * state.
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
        mReferenceTracker.unregisterResource(resource);
        break;
      }
      default: throw new InternalKijiError(String.format(
          "Unknown DebugResourceTracker.TrackingType: %s", TRACKING_LEVEL));
    }
  }

  /**
   * Tracks registered resources. Uses the phantom reference mechanism of the JVM to recognize when
   * registered resources are no longer reachable, and logs them. Will log any registered resources
   * when closed.
   */
  private static final class ReferenceTracker implements Closeable {
    private final ExecutorService mExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("DebugResourceTracker.ReferenceTracker")
                .build());

    /**
     * Create a {@code ReferenceTracker} instance.
     */
    private ReferenceTracker() {
      mExecutorService.execute(new ReferenceLogger());
    }

    /** Ref queue for showable phantom references. */
    private final ReferenceQueue<Object> mReferenceQueue =
        new ReferenceQueue<Object>();

    /** Map of identity hash code of registered referent to reference. Synchronized. */
    private final BiMap<Integer, ShowablePhantomReference>  mReferences =
        Maps.synchronizedBiMap(HashBiMap.<Integer, ShowablePhantomReference>create());

    /**
     * Register a resource to be tracked.
     *
     * @param resource to be registered to this tracker.
     * @param message associated with the resource.
     * @param stackTrace associated with the resource.
     */
    public void registerResource(
        final Object resource,
        final String message,
        final String stackTrace
    ) {
      LOG.debug("Registering resource {}.", resource);
      final Object previous = mReferences.put(
          System.identityHashCode(resource),
          new ShowablePhantomReference(mReferenceQueue, resource, message, stackTrace));
      if (previous != null) {
        CLEANUP_LOG.warn(
            "Hash collision (or double-registration) for resource {}: {}.",
            resource, message);
      }
    }

    /**
     * Unregister a resource from this tracker.
     *
     * @param resource to be unregistered from this tracker.
     */
    public void unregisterResource(final Object resource) {
      LOG.debug("Unregistering resource {}.", resource);
      final Object removed = mReferences.remove(System.identityHashCode(resource));
      if (removed == null) {
        CLEANUP_LOG.warn("Attempted to unregister an untracked resource: {}.", resource);
      }
    }

    /**
     * Close this {@code ReferenceTracker}.  Will log any outstanding registered resources.
     */
    @Override
    public void close() {
      mExecutorService.shutdownNow();
      synchronized (mReferences) {
        for (ShowablePhantomReference reference : mReferences.values()) {
          logReference(reference);
        }
        mReferences.clear();
      }
    }

    /**
     * Log a reference in the cleanup log with it's message.
     *
     * @param reference to log.
     */
    private static void logReference(ShowablePhantomReference reference) {
      CLEANUP_LOG.error("Leaked resource detected: {}\n{}",
          reference.getMessage(),
          reference.getStackTrace());
    }

    /**
     * Task which waits for ShowablePhantomRef instances to be enqueued to the reference queue, and
     * logs them.
     */
    private class ReferenceLogger implements Runnable {
      /** {@inheritDoc} */
      @Override
      public void run() {
        try {
          while (true) {
            ShowablePhantomReference reference =
                (ShowablePhantomReference) mReferenceQueue.remove();
            if (mReferences.inverse().remove(reference) != null) {
              // Log reference if it is still registered
              logReference(reference);
            }
            reference.clear(); // allows referent to be claimed by the GC.
          }
        } catch (InterruptedException e) {
          // If this thread is interrupted, then die. This happens normally when
          // ReferenceTracker#close is called.

          // Restore the interrupted status
          Thread.currentThread().interrupt();
        }
      }
    }

    /**
     * A {@link PhantomReference} which holds the {@code #toString} of its referent and a message.
     */
    @ApiAudience.Private
    private static final class ShowablePhantomReference extends PhantomReference<Object> {

      /** Message associated with the referent. */
      private final String mMessage;

      /** Stack trace asscociated with the referent. */
      private final String mStackTrace;

      /**
       * Create a {@code ShowablePhantomReference} for the provided queue, referent, message, and
       * stack trace.
       *
       * @param refQueue to which this reference will be enqueued when the JVM determines the
       *    referent is no longer reachable.
       * @param referent of this phantom reference.
       * @param message associated with this reference.
       * @param stackTrace associated with this reference.
       */
      public ShowablePhantomReference(
          final ReferenceQueue<Object> refQueue,
          final Object referent,
          final String message,
          final String stackTrace
      ) {
        super(referent, refQueue);
        mMessage = message;
        mStackTrace = stackTrace;
      }

      /**
       * @return the message associated with the referent.
       */
      public String getMessage() {
        return mMessage;
      }

      /**
       * @return the stack trace associated with the referent.
       */
      public String getStackTrace() {
        return mStackTrace;
      }

      /** {@inheritDoc} */
      @Override
      public String toString() {
        return Objects
            .toStringHelper(this.getClass())
            .add("message", mMessage)
            .add("stack trace", mStackTrace)
            .toString();
      }
    }
  }
}
