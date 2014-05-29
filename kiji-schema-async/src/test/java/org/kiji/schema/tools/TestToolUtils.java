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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdException;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.HBaseEntityId;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestToolUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TestToolUtils.class);

  public TestToolUtils() throws IOException {
  }

  private final KijiTableLayout mFormattedLayout =
      KijiTableLayouts.getTableLayout(KijiTableLayouts.FORMATTED_RKF);

  private final KijiTableLayout mRawLayout =
      KijiTableLayouts.getTableLayout(KijiTableLayouts.SQOOP_EXPORT_SAMPLING_UNHASHED_TEST);

  private final KijiTableLayout mHashedLayout =
      KijiTableLayouts.getTableLayout(KijiTableLayouts.SQOOP_EXPORT_SAMPLING_HASHED_TEST);

  private final KijiTableLayout mHashPrefixedLayout =
      KijiTableLayouts.getTableLayout(KijiTableLayouts.HASH_PREFIXED_RKF);

  // -----------------------------------------------------------------------------------------------

  /** FormattedEntityId. */

  @Test
  public void testTooLargeInt() throws Exception {
    try {
      ToolUtils.createEntityIdFromUserInputs("['dummy', 'str1', 'str2', 2147483648, 10]",
          mFormattedLayout);
      fail("Should fail with EntityIdException.");
    } catch (EntityIdException eie) {
      assertEquals("Invalid type for component 2147483648 at index 3 in kijiRowKey",
          eie.getMessage());
    }
  }

  @Test
  public void testTooLargeLong() throws Exception {

    try {
      ToolUtils.createEntityIdFromUserInputs("['dummy', 'str1', 'str2', 5, 9223372036854775808]",
          mFormattedLayout);
      fail("Should fail with EntityIdException.");
    } catch (IOException ioe) {
      assertEquals("Invalid JSON value: '9223372036854775808', expecting string, "
          + "int, long, or null.", ioe.getMessage());
    }
  }

  @Test
  public void testIntForLong() throws Exception {
    final EntityIdFactory factory = EntityIdFactory.getFactory((RowKeyFormat2) mFormattedLayout
        .getDesc().getKeysFormat());
    final EntityId eid = factory.getEntityId("dummy", "str1", "str2", 5, 10);

    assertEquals(eid, ToolUtils.createEntityIdFromUserInputs("['dummy', 'str1', 'str2', 5, 10]",
        mFormattedLayout));
  }

  @Test
  public void testTooFewComponents() throws IOException {
    final EntityIdFactory factory =
        EntityIdFactory.getFactory((RowKeyFormat2) mFormattedLayout.getDesc().getKeysFormat());
    final EntityId eid = factory.getEntityId("dummy", "str1", "str2", 5, null);

    assertEquals(eid,
        ToolUtils.createEntityIdFromUserInputs("['dummy', 'str1', 'str2', 5]", mFormattedLayout));
  }

  @Test
  public void testNonNullFollowsNull() throws IOException {

    try {
      ToolUtils
          .createEntityIdFromUserInputs("['dummy', 'str1', 'str2', null, 5]", mFormattedLayout);
      fail("Should fail with EntityIdException.");
    } catch (EntityIdException eie) {
      assertEquals("Non null component follows null component", eie.getMessage());
    }
  }

  @Test
  public void testEmptyString() throws IOException {
    final EntityIdFactory factory =
      EntityIdFactory.getFactory((RowKeyFormat2) mFormattedLayout.getDesc().getKeysFormat());
    final EntityId eid = factory.getEntityId("", "", "", null, null);

    assertEquals(eid,
        ToolUtils.createEntityIdFromUserInputs("['', '', '', null, null]", mFormattedLayout));
  }

  @Test
  public void testASCIIChars() throws Exception {
    final EntityIdFactory factory =
        EntityIdFactory.getFactory((RowKeyFormat2) mFormattedLayout.getDesc().getKeysFormat());

    for (byte b = 32; b < 127; b++) {
      for (byte b2 = 32; b2 < 127; b2++) {
        final EntityId eid = factory.getEntityId(String.format(
            "dumm%sy", new String(new byte[]{b, b2}, "Utf-8")), "str1", "str2", 5, 10L);

        assertEquals(eid,
            ToolUtils.createEntityIdFromUserInputs(eid.toShellString(), mFormattedLayout));
      }
    }
  }

  /** RawEntityId. */

  @Test
  public void testRawParserLoop() throws Exception {
    final EntityIdFactory factory =
      EntityIdFactory.getFactory((RowKeyFormat) mRawLayout.getDesc().getKeysFormat());
    final EntityId eid = factory.getEntityIdFromHBaseRowKey(Bytes.toBytes("rawRowKey"));

    assertEquals(eid,
        ToolUtils.createEntityIdFromUserInputs(eid.toShellString(), mRawLayout));
  }

  /** HashedEntityId. */

  @Test
  public void testHashedParserLoop() throws Exception {
    final EntityIdFactory factory =
       EntityIdFactory.getFactory((RowKeyFormat) mHashedLayout.getDesc().getKeysFormat());
    final EntityId eid = factory.getEntityId("hashedRowKey");

    assertEquals(eid,
        ToolUtils.createEntityIdFromUserInputs(eid.toShellString(), mHashedLayout));
  }

  /** HashPrefixedEntityId. */

  @Test
  public void testHashPrefixedParserLoop() throws Exception {
    final EntityIdFactory factory =
       EntityIdFactory.getFactory((RowKeyFormat) mHashPrefixedLayout.getDesc().getKeysFormat());
    final EntityId eid = factory.getEntityId("hashPrefixedRowKey");

    assertEquals(
        eid, ToolUtils.createEntityIdFromUserInputs(eid.toShellString(), mHashPrefixedLayout));
  }

  /** HBaseEntityId. */

  @Test
  public void testHBaseEIDtoRawEID() throws Exception {
    final EntityIdFactory factory =
      EntityIdFactory.getFactory((RowKeyFormat) mRawLayout.getDesc().getKeysFormat());
    final EntityId reid = factory.getEntityId("rawEID");
    final EntityId hbeid = HBaseEntityId.fromHBaseRowKey(reid.getHBaseRowKey());

    assertEquals(reid,
        ToolUtils.createEntityIdFromUserInputs(hbeid.toShellString(), mRawLayout));
  }

  @Test
  public void testHBaseEIDtoHashedEID() throws Exception {
    final EntityIdFactory factory =
      EntityIdFactory.getFactory((RowKeyFormat) mHashedLayout.getDesc().getKeysFormat());
    final EntityId heid = factory.getEntityId("hashedEID");
    final EntityId hbeid = HBaseEntityId.fromHBaseRowKey(heid.getHBaseRowKey());

    assertEquals(heid,
        ToolUtils.createEntityIdFromUserInputs(hbeid.toShellString(), mHashedLayout));

  }

  @Test
  public void testHBaseEIDtoHashPrefixedEID() throws Exception {
    final EntityIdFactory factory =
      EntityIdFactory.getFactory((RowKeyFormat) mHashPrefixedLayout.getDesc().getKeysFormat());
    final EntityId hpeid = factory.getEntityId("hashPrefixedEID");
    final EntityId hbeid = HBaseEntityId.fromHBaseRowKey(hpeid.getHBaseRowKey());

    assertEquals(hpeid,
        ToolUtils.createEntityIdFromUserInputs(hbeid.toShellString(), mHashPrefixedLayout));
  }

  @Test
  public void testHBaseEIDtoFormattedEID() throws Exception {
    final EntityIdFactory factory =
        EntityIdFactory.getFactory((RowKeyFormat2) mFormattedLayout.getDesc().getKeysFormat());
    final EntityId feid = factory.getEntityId("dummy", "str1", "str2", 5, 10);
    final EntityId hbeid = HBaseEntityId.fromHBaseRowKey(feid.getHBaseRowKey());

    assertEquals(feid,
        ToolUtils.createEntityIdFromUserInputs(hbeid.toShellString(), mFormattedLayout));
  }
}
