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

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

public class TestNumberParser {
  private static final String INFINITY = "infinity";
  /** Number of times to pass random numbers to NumberParser during tests. */
  private static final int NUM_TESTS = 10;

  @Test
  public void parseIntInfinity() {
    int infty = NumberParser.parseInt(INFINITY);
    assertEquals(Integer.MAX_VALUE, infty);
  }

  @Test
  public void parseInt() {
    Random rand = new Random();
    for (int i = 0; i < NUM_TESTS; i++) {
      int anInt = rand.nextInt();
      assertEquals(anInt, NumberParser.parseInt(Integer.toString(anInt)));
    }
    assertEquals(Integer.MAX_VALUE, NumberParser.parseInt(Integer.toString(Integer.MAX_VALUE)));
  }

  @Test
  public void parseLongInfinity() {
    long infty = NumberParser.parseLong(INFINITY);
    assertEquals(Long.MAX_VALUE, infty);
  }

  @Test
  public void parseLong() {
    Random rand = new Random();
    for (int i = 0; i < NUM_TESTS; i++) {
      long aLong = rand.nextLong();
      assertEquals(aLong, NumberParser.parseLong(Long.toString(aLong)));
    }
    assertEquals(Long.MAX_VALUE, NumberParser.parseLong(Long.toString(Long.MAX_VALUE)));
  }
}
