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

import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;

public class TestOperatorRowFilter {

  @Test
  public void testEqualsAndHashCode() {
    final KijiRowFilter stripValFilter = new StripValueRowFilter();
    final KijiRowFilter hasColDataFilter =
        new HasColumnDataRowFilter("family", "qualifier");
    final List<KijiRowFilter> filterList1 =
        Lists.newArrayList(stripValFilter);
    final List<KijiRowFilter> filterList2a =
        Lists.newArrayList(stripValFilter, hasColDataFilter);
    final List<KijiRowFilter> filterList2b =
        Lists.newArrayList(stripValFilter, hasColDataFilter);
    final List<KijiRowFilter> filterList2Reversed =
        Lists.newArrayList(hasColDataFilter, stripValFilter);

    final OperatorRowFilter andFilter1 = new AndRowFilter(filterList1);
    final OperatorRowFilter andFilter2a = new AndRowFilter(filterList2a);
    final OperatorRowFilter andFilter2b = new AndRowFilter(filterList2b);
    final OperatorRowFilter andFilter2Reversed = new AndRowFilter(filterList2Reversed);
    final OperatorRowFilter orFilter = new OrRowFilter(filterList2a);

    assertEquals(andFilter2a, andFilter2b);
    assertFalse(andFilter1.equals(andFilter2a));
    assertFalse(andFilter2a.equals(orFilter));
    assertFalse(andFilter2a.equals(andFilter2Reversed));

    assertEquals(andFilter2a.hashCode(), andFilter2b.hashCode());
  }
}
