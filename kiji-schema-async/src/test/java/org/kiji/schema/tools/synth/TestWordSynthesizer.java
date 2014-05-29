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

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

public class TestWordSynthesizer {

  @Test
  public void testWordSynth() {
    Random random = createStrictMock(Random.class);
    expect(random.nextInt(3)).andReturn(0);
    expect(random.nextInt(3)).andReturn(1);

    List<String> dictionary = new ArrayList<String>(3);
    dictionary.add("a");
    dictionary.add("b");
    dictionary.add("c");

    replay(random);
    WordSynthesizer synth = new WordSynthesizer(random, dictionary);
    assertEquals("a", synth.synthesize());
    assertEquals("b", synth.synthesize());
    verify(random);
  }
}
