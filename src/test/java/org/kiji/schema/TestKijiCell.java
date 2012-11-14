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

package org.kiji.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestKijiCell {
  @Test
  public void testEquals() {
    KijiCell<Integer> a = new KijiCell<Integer>(Schema.create(Schema.Type.INT), new Integer(5));
    KijiCell<Integer> b = new KijiCell<Integer>(Schema.create(Schema.Type.INT), new Integer(5));
    KijiCell<Integer> c = new KijiCell<Integer>(Schema.create(Schema.Type.INT), new Integer(6));
    KijiCell<Long> d = new KijiCell<Long>(Schema.create(Schema.Type.LONG), new Long(5));

    assertEquals(a, b);
    assertThat(a, is(not(c)));
    assertFalse(a.equals(d));

    KijiCell<CharSequence> e = new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING),
        new String("foo"));
    KijiCell<CharSequence> f = new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING),
        new Utf8("foo"));
    assertEquals(e, f);
  }
}
