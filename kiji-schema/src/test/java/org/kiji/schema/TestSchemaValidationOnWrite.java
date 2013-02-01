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

import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.avro.Edge;
import org.kiji.schema.avro.Node;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

/** Tests the schema validation on write. */
public class TestSchemaValidationOnWrite extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestSchemaValidationOnWrite.class);

  @Test
  public void testWrite() throws Exception {
    final Kiji kiji = getKiji();

    final TableLayoutDesc layoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.FULL_FEATURED);
    final KijiTableLayout tableLayout = new KijiTableLayout(layoutDesc, null);
    kiji.createTable("user", tableLayout);

    final KijiTable table = kiji.openTable("user");
    final KijiTableWriter writer = table.openTableWriter();

    final EntityId eid = table.getEntityId("row-key");

    // info:name is typed as "string" and must be a CharSequence:
    writer.put(eid, "info", "name", "The user name");
    try {
      writer.put(eid, "info", "name", 1L);
      fail("Writing a long instead of a string should fail.");
    } catch (KijiEncodingException kee) {
      LOG.info("Expected error: " + kee);
    }

    // recommendations:product must be a Node record:
    final Node node = Node.newBuilder()
        .setLabel("label")
        .setWeight(1.0)
        .setEdges(null)
        .setAnnotations(null)
        .build();
    final Edge edge = Edge.newBuilder()
        .setLabel("label")
        .setWeight(1.0)
        .setTarget(node)
        .setAnnotations(null)
        .build();

    writer.put(eid, "recommendations", "product", node);
    try {
      writer.put(eid, "recommendations", "product", "a bogus value");
      fail("Writing a string instead of a Node should fail.");
    } catch (KijiEncodingException kee) {
      LOG.info("Expected error: " + kee);
    }
    try {
      writer.put(eid, "recommendations", "product", 1L);
      fail("Writing a long instead of a Node should fail.");
    } catch (KijiEncodingException kee) {
      LOG.info("Expected error: " + kee);
    }
    try {
      writer.put(eid, "recommendations", "product", table.getLayout().getDesc());
      fail("Writing an TableLayoutDesc instead of a Node should fail.");
    } catch (KijiEncodingException kee) {
      LOG.info("Expected error: " + kee);
    }
    try {
      writer.put(eid, "recommendations", "product", edge);
      fail("Writing an Edge instead of a Node should fail.");
    } catch (KijiEncodingException kee) {
      LOG.info("Expected error: " + kee);
    }

    writer.close();
    table.close();
  }
}
