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

package org.kiji.schema.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

public class TestRegexQualifierColumnFilter {
  @Test
  public void testEqualsAndHashCode() {
    final String regexString1 = "myregex";
    final String regexString2 = "my second regex";
    final RegexQualifierColumnFilter filter1a =
        new RegexQualifierColumnFilter(regexString1);
    final RegexQualifierColumnFilter filter1b =
        new RegexQualifierColumnFilter(regexString1);
    final RegexQualifierColumnFilter filter2 =
        new RegexQualifierColumnFilter(regexString2);

    assertEquals(filter1a, filter1b);
    assertFalse(filter1a.equals(filter2));

    assertEquals(filter1a.hashCode(), filter1b.hashCode());
  }
}
