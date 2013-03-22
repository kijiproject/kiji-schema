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

import java.util.List;
import java.util.Random;

import org.kiji.annotations.ApiAudience;

/**
 * Synthesizes random words from a dictionary.
 */
@ApiAudience.Private
public class WordSynthesizer implements Synthesizer<String> {
  /** A random number generator. */
  private Random mRandom;
  /** A word dictionary. */
  private List<String> mDictionary;

  /**
   * Constructs a word synthesizer.
   *
   * @param random A random number generator.
   * @param dictionary A vocabulary of words to use when synthesizing.
   */
  public WordSynthesizer(Random random, List<String> dictionary) {
    mRandom = random;
    mDictionary = dictionary;
  }

  @Override
  public String synthesize() {
    return mDictionary.get(mRandom.nextInt(mDictionary.size()));
  }
}
