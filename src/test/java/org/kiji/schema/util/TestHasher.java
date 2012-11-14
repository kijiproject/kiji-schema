/**
 * (c) Copyright 2012 WibiData, Inc.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHasher {
  private static final Logger LOG = LoggerFactory.getLogger(TestHasher.class);

  @Test
  public void testHash() {
    assertArrayEquals(Hasher.hash("foo"), Hasher.hash("foo"));

    // 16 bytes = 128-bit MD5.
    assertEquals(16, Hasher.hash("bar").length);

    assertFalse(Arrays.equals(Hasher.hash("foo"), Hasher.hash("bar")));
  }

  public class HashingThread extends Thread {
    private final int mIterations;
    private boolean mFailed;

    public HashingThread(int iterations) {
      mIterations = iterations;
      mFailed = false;
    }

    @Override
    public void run() {
      LOG.debug("HashingThread.run() started.");
      for (int i = 0; i < mIterations; i++) {
        String input = UUID.randomUUID().toString();
        if (!Arrays.equals(Hasher.hash(input), Hasher.hash(input))) {
          mFailed = true;
        }
      }
      LOG.debug("HashingThread.run() complete.");
    }

    /** @return Whether the hasher thread failed to hash consistently. */
    public boolean failed() {
      return mFailed;
    }
  }

  @Test
  public void testHashMultiThreaded() {
    final int numThreads = 4;
    final int numIterations = 48;

    // Create a bunch of threads that hash in a loop.
    List<HashingThread> hashingThreads = new ArrayList<HashingThread>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      hashingThreads.add(new HashingThread(numIterations));
    }

    // Start the threads.
    for (Thread thread : hashingThreads) {
      thread.start();
    }

    // Wait for the threads.
    for (Thread thread : hashingThreads) {
      while (thread.isAlive()) {
        try {
          thread.join();
        } catch (InterruptedException e) {
          LOG.debug("Main thread interrupted while waiting for children to complete...");
        }
      }
    }

    // Make sure all the threads succeeded.
    for (HashingThread thread : hashingThreads) {
      assertFalse(thread.failed());
    }
  }
}
