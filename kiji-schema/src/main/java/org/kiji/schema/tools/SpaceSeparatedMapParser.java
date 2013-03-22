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

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

import org.kiji.annotations.ApiAudience;

/**
 * Parser for space-separated map arguments.
 *
 * Space-separated maps are specified from the command-line shell, as:
 * <pre> kiji tool --flag="key1=value1 key2=value2 key3=value3" </pre>
 *
 * When only one value is needed, this can be shortened as:
 * <pre> kiji tool --flag=key=value </pre>
 *
 * <li> Keys must be unique.
 * <li> Values may not contain spaces.
 * <li> Values will typically be URLs or URIs and may include spaces escaped as "%20".
 */
@ApiAudience.Private
public final class SpaceSeparatedMapParser {

  /** Initializes a space-separated value parse. */
  private SpaceSeparatedMapParser() {
  }

  /** @return a new parser for space-separated parameter maps. */
  public static SpaceSeparatedMapParser create() {
    return new SpaceSeparatedMapParser();
  }

  /**
   * Parses a space-separated map into a Java map.
   *
   * @param input Space-separated map.
   * @return the parsed Java map.
   * @throws IOException on parse error.
   */
  public Map<String, String> parse(String input) throws IOException {
    final Map<String, String> kvs = Maps.newHashMap();

    for (String pair: input.split(" ")) {
      if (pair.isEmpty()) {
        continue;
      }
      final String[] kvSplit = pair.split("=", 2);
      if (kvSplit.length != 2) {
        throw new IOException(String.format(
            "Invalid argument '%s' in space-separated map '%s'.", pair, input));
      }

      final String key = kvSplit[0];
      final String value = kvSplit[1];

      final String existing = kvs.put(key, value);
      if (existing != null) {
        throw new IOException(String.format(
            "Duplicate key '%s' in space-separated map.'%s'.", key, input));
      }
    }

    return kvs;
  }
}
