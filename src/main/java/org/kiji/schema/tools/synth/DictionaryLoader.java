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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.kiji.annotations.ApiAudience;

/**
 * Loads a dictionary file, which is simply a list of words, one per line.
 */
@ApiAudience.Private
public final class DictionaryLoader {
  /**
   * Creates a new <code>DictionaryLoader</code> instance.
   */
  public DictionaryLoader() {
  }

  /**
   * Loads the dictionary from a file.
   *
   * @param filename The path to a file of words, one per line.
   * @return The list of words from the file.
   * @throws IOException If there is an error reading the words from the file.
   */
  public List<String> load(String filename) throws IOException {
    FileInputStream fileStream = new FileInputStream(filename);
    try {
      return load(fileStream);
    } finally {
      fileStream.close();
    }
  }

  /**
   * Loads the dictionary from an input stream.
   *
   * @param inputStream The input stream of words, one per line.
   * @return The list of words from the stream.
   * @throws IOException If there is an error reading the words from the stream.
   */
  public List<String> load(InputStream inputStream) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
    List<String> dict = new ArrayList<String>();
    while (reader.ready()) {
      dict.add(StringUtils.strip(reader.readLine()));
    }
    return dict;
  }
}
