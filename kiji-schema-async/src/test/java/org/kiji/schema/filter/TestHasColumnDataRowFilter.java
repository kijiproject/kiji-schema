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

public class TestHasColumnDataRowFilter {

  @Test
  public void testEqualsAndHashCode() {
    final HasColumnDataRowFilter filter1 =
        new HasColumnDataRowFilter("family", "qualifier");
    final HasColumnDataRowFilter filter2 =
        new HasColumnDataRowFilter("family", "qualifier");
    final HasColumnDataRowFilter differentFilter =
        new HasColumnDataRowFilter("differentfamily", "qualifier");
    assertEquals(filter1, filter2);
    assertFalse(filter1.equals(differentFilter));

    assertEquals(filter1.hashCode(), filter2.hashCode());
  }
}
