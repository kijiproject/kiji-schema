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

package org.kiji.schema.tools.synth;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

public class TestDictionaryLoader {
  @Test
  public void testLoad() throws IOException {
    DictionaryLoader loader = new DictionaryLoader();
    List<String> dictionary = loader.load(getClass().getClassLoader().getResourceAsStream(
            "org/kiji/schema/tools/synth/dictionary.txt"));
    assertEquals(3, dictionary.size());
    assertEquals("apple", dictionary.get(0));
    assertEquals("banana", dictionary.get(1));
    assertEquals("carrot", dictionary.get(2));
  }
}
