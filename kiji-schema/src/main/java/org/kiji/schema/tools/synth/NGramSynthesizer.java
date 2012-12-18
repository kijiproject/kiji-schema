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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.kiji.annotations.ApiAudience;

/**
 * Generates n-grams.
 */
@ApiAudience.Private
public final class NGramSynthesizer implements Synthesizer<String> {
  /** The number of words in the n-gram. */
  private int mN;
  /** A word synthesizer. */
  private WordSynthesizer mWordSynthesizer;

  /**
   * Constructs a new n-gram synthesizer.
   *
   * @param wordSynthesizer A synthesizer to generate the words in the n-grams.
   * @param n The number of words in the n-gram.
   */
  public NGramSynthesizer(WordSynthesizer wordSynthesizer, int n) {
    mN = n;
    mWordSynthesizer = wordSynthesizer;
  }

  @Override
  public String synthesize() {
    // Pick n words from the dictionary.
    List<String> grams = new ArrayList<String>(mN);
    for (int i = 0; i < mN; i++) {
      grams.add(mWordSynthesizer.synthesize());
    }
    return StringUtils.join(grams, " ");
  }
}
