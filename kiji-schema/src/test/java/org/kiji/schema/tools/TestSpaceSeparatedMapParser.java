/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.schema.tools;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for SpaceSeparatedMapParser. */
public class TestSpaceSeparatedMapParser {
  private static final Logger LOG = LoggerFactory.getLogger(TestSpaceSeparatedMapParser.class);

  private SpaceSeparatedMapParser mParser = SpaceSeparatedMapParser.create();

  @Test
  public void testParseValidMapEmpty() throws Exception {
    final Map<String, String> kvs = mParser.parse("");
    final Map<String, String> expected = ImmutableMap.<String, String>builder().build();
    assertEquals(expected, kvs);
  }

  @Test
  public void testParseValidMapOneEntry() throws Exception {
    final Map<String, String> kvs = mParser.parse("key1=value1");
    final Map<String, String> expected =
        ImmutableMap.<String, String>builder().put("key1", "value1").build();
    assertEquals(expected, kvs);
  }

  @Test
  public void testParseValidMapManyEntries() throws Exception {
    final Map<String, String> kvs = mParser.parse("key1=value1 key2=value2 key3=value3");
    final Map<String, String> expected = ImmutableMap.<String, String>builder()
        .put("key1", "value1")
        .put("key2", "value2")
        .put("key3", "value3")
        .build();
    assertEquals(expected, kvs);
  }

  @Test
  public void testParseValidMapEmptySpaces() throws Exception {
    final Map<String, String> kvs = mParser.parse("    ");
    final Map<String, String> expected = ImmutableMap.<String, String>builder().build();
    assertEquals(expected, kvs);
  }

  @Test
  public void testParseValidMapManySpaces() throws Exception {
    final Map<String, String> kvs = mParser.parse(" key1=value1   key2=value2  key3=value3   ");
    final Map<String, String> expected = ImmutableMap.<String, String>builder()
        .put("key1", "value1")
        .put("key2", "value2")
        .put("key3", "value3")
        .build();
    assertEquals(expected, kvs);
  }

  // ----------------------------------------------------------------------------------------------
  // Invalid scenarios

  @Test
  public void testParseInvalidMapNoEquals() throws Exception {
    try {
      final String input = "key1=value1 invalid key3=value3";
      mParser.parse(input);
      fail(String.format("Space-separated parser should have caught an error in '%s'", input));
    } catch (IOException ioe) {
      LOG.debug("Expected error: {}", ioe);
    }
  }

  @Test
  public void testParseInvalidMapDuplicateKey() throws Exception {
    try {
      final String input = "key1=value1 key1=value2";
      mParser.parse(input);
      fail(String.format("Space-separated parser should have caught an error in '%s'", input));
    } catch (IOException ioe) {
      LOG.debug("Expected error: {}", ioe);
    }
  }
}
