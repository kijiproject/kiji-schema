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

package org.kiji.schema;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

public class TestKijiCell {

  @Test
  public void testEquals() {
    final KijiCell<Integer> cell1 =
        KijiCell.create(KijiColumnName.create("family", "qualifier"), 1234L,
            new DecodedCell<Integer>(Schema.create(Schema.Type.INT), 31415));

    final KijiCell<Integer> cell2 =
        KijiCell.create(KijiColumnName.create("family", "qualifier"), 1234L,
            new DecodedCell<Integer>(Schema.create(Schema.Type.INT), 31415));

    Assert.assertTrue(cell1.equals(cell2));
    Assert.assertTrue(cell2.equals(cell1));

    Assert.assertEquals(cell1.hashCode(), cell2.hashCode());
  }

  @Test
  public void testEqualsFamilyMismatch() {
    final KijiCell<Integer> cell1 =
        KijiCell.create(KijiColumnName.create("family", "qualifier"), 1234L,
            new DecodedCell<Integer>(Schema.create(Schema.Type.INT), 31415));

    final KijiCell<Integer> cell2 =
        KijiCell.create(KijiColumnName.create("other", "qualifier"), 1234L,
            new DecodedCell<Integer>(Schema.create(Schema.Type.INT), 31415));

    Assert.assertFalse(cell1.equals(cell2));
  }

  @Test
  public void testEqualsQualifierMismatch() {
    final KijiCell<Integer> cell1 =
        KijiCell.create(KijiColumnName.create("family", "qualifier"), 1234L,
            new DecodedCell<Integer>(Schema.create(Schema.Type.INT), 31415));

    final KijiCell<Integer> cell2 =
        KijiCell.create(KijiColumnName.create("family", "other"), 1234L,
            new DecodedCell<Integer>(Schema.create(Schema.Type.INT), 31415));

    Assert.assertFalse(cell1.equals(cell2));
  }

  @Test
  public void testEqualsTimestampMismatch() {
    final KijiCell<Integer> cell1 =
        KijiCell.create(KijiColumnName.create("family", "qualifier"), 1234L,
            new DecodedCell<Integer>(Schema.create(Schema.Type.INT), 31415));

    final KijiCell<Integer> cell2 =
        KijiCell.create(KijiColumnName.create("family", "qualifier"), 1235L,
            new DecodedCell<Integer>(Schema.create(Schema.Type.INT), 31415));

    Assert.assertFalse(cell1.equals(cell2));
  }

  @Test
  public void testEqualsValueContentMismatch() {
    final KijiCell<Integer> cell1 =
        KijiCell.create(KijiColumnName.create("family", "qualifier"), 1234L,
            new DecodedCell<Integer>(Schema.create(Schema.Type.INT), 31415));

    final KijiCell<Integer> cell2 =
        KijiCell.create(KijiColumnName.create("family", "qualifier"), 1234L,
            new DecodedCell<Integer>(Schema.create(Schema.Type.INT), 0));

    Assert.assertFalse(cell1.equals(cell2));
  }

  @Test
  public void testEqualsValueSchemaMismatch() {
    final KijiCell<Integer> cell1 =
        KijiCell.create(KijiColumnName.create("family", "qualifier"), 1234L,
            new DecodedCell<Integer>(Schema.create(Schema.Type.INT), 31415));

    final KijiCell<Long> cell2 =
        KijiCell.create(KijiColumnName.create("family", "qualifier"), 1235L,
            new DecodedCell<Long>(Schema.create(Schema.Type.LONG), 31415L));

    Assert.assertFalse(cell1.equals(cell2));
  }
}
