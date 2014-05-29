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

package org.kiji.schema.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import org.kiji.schema.impl.KijiPaginationFilter;

public class TestOperatorColumnFilter {

  @Test
  public void testEqualsAndHashCode() {
    final KijiColumnFilter fColRange = KijiColumnRangeFilter.greaterThan("a");
    final KijiColumnFilter fPage = new KijiPaginationFilter(1);

    final OperatorColumnFilter andFilter1 = new AndColumnFilter(fColRange);
    final OperatorColumnFilter andFilter2a = new AndColumnFilter(fColRange, fPage);
    final OperatorColumnFilter andFilter2b = new AndColumnFilter(fColRange, fPage);
    final OperatorColumnFilter andFilter2Reversed = new AndColumnFilter(fPage, fColRange);
    final OperatorColumnFilter orFilter = new OrColumnFilter(fColRange, fPage);

    assertEquals(andFilter2a, andFilter2b);
    assertFalse(andFilter1.equals(andFilter2a));
    assertFalse(andFilter2a.equals(orFilter));
    assertFalse(andFilter2a.equals(andFilter2Reversed));

    assertEquals(andFilter2a.hashCode(), andFilter2b.hashCode());
  }
}
