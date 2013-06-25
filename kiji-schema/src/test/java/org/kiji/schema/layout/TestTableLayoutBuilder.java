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

package org.kiji.schema.layout;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.avro.Schema;
import org.junit.Test;

import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.avro.TableLayoutDesc;

public class TestTableLayoutBuilder extends KijiClientTest {

  /**
   * Only deep copies should be mutated by TableLayoutBuilder.
   *
   * @throws IOException
   */
  @Test
  public void testTableLayoutSafeMutation() throws IOException {
    final KijiTableLayout layout = KijiTableLayouts
        .getTableLayout(KijiTableLayouts.SCHEMA_REG_TEST);
    final TableLayoutDesc tld = layout.getDesc();
    final TableLayoutBuilder tlb = new TableLayoutBuilder(tld, getKiji());
    tld.setName("blastoise");
    final TableLayoutDesc tldBuilt = tlb.build();
    assertFalse(tld.getName().equals(tldBuilt.getName()));
  }

  @Test
  public void testSchemaRegistration() throws IOException {
    // Set up schemas
    final Schema.Parser p = new Schema.Parser();
    final Schema stringSchema = p.parse("\"string\"");
    final Schema intSchema = p.parse("\"int\"");
    final Schema enumSchema = p.parse("{ \"type\": \"enum\", \"name\": \"HeroType\", "
        + "\"symbols\" : [\"Paladin\", \"Mage\", \"Orck\", \"Gelf\"]}");
    final Schema fixedSchema =
        p.parse("{\"type\": \"fixed\", \"size\": 16, \"name\":\"some_fixed\"}");
    final Schema unionSchema =
        p.parse("[\"null\", \"string\", \"some_fixed\"]");

    // Set up layout
    final KijiTableLayout layout = KijiTableLayouts
        .getTableLayout(KijiTableLayouts.SCHEMA_REG_TEST);
    final TableLayoutBuilder tlb = new TableLayoutBuilder(layout.getDesc(), getKiji());

    // Columns to use
    final KijiColumnName fullNameCol = new KijiColumnName("info:fullname");
    final KijiColumnName hitPointsCol = new KijiColumnName("info:hitpoints");
    final KijiColumnName manaCol = new KijiColumnName("info:mana");
    final KijiColumnName friendsCol = new KijiColumnName("friends");
    final KijiColumnName questProgressCol = new KijiColumnName("quest_progress");

    // Aliases for above columns, respectively
    final KijiColumnName aliasCol = new KijiColumnName("profile:alias");
    final KijiColumnName healthCol = new KijiColumnName("info:health");
    final KijiColumnName magicCol = new KijiColumnName("profile:magic");
    final KijiColumnName fellowsCol = new KijiColumnName("fellows");
    final KijiColumnName heroismCol = new KijiColumnName("heroism");

    // Check emptiness (not exhaustive)
    // TODO: integrate default schemas
    assertTrue(tlb.getRegisteredReaders(fullNameCol).isEmpty());
    assertTrue(tlb.getRegisteredReaders(healthCol).isEmpty());
    assertTrue(tlb.getRegisteredReaders(magicCol).isEmpty());
    assertTrue(tlb.getRegisteredWriters(fullNameCol).isEmpty());
    assertTrue(tlb.getRegisteredWriters(fellowsCol).isEmpty());
    assertTrue(tlb.getRegisteredWriters(questProgressCol).isEmpty());
    assertTrue(tlb.getRegisteredWritten(aliasCol).isEmpty());
    assertTrue(tlb.getRegisteredWritten(hitPointsCol).isEmpty());
    assertTrue(tlb.getRegisteredWritten(manaCol).isEmpty());

    // Register/deregister schemas
    tlb.withReader(fullNameCol, stringSchema)
        .withWriter(hitPointsCol, fixedSchema)
        .withWritten(manaCol, enumSchema)
        .withReader(friendsCol, intSchema)
        .withReader(friendsCol, unionSchema)
        .withReader(friendsCol, enumSchema)
        .withoutReader(friendsCol, enumSchema)
        .withWriter(questProgressCol, unionSchema);

    tlb.withoutReader(heroismCol, stringSchema)
       .withReader(heroismCol, intSchema)
       .withoutReader(heroismCol, unionSchema)
       .withReader(heroismCol, enumSchema);

    // Check successful registration/deregistration
    assertTrue(tlb.getRegisteredReaders(fullNameCol).contains(stringSchema));
    assertTrue(tlb.getRegisteredWriters(hitPointsCol).contains(fixedSchema));
    assertTrue(tlb.getRegisteredWritten(manaCol).contains(enumSchema));
    assertTrue(tlb.getRegisteredReaders(friendsCol).contains(intSchema));
    assertTrue(tlb.getRegisteredReaders(friendsCol).contains(unionSchema));
    assertTrue(!tlb.getRegisteredReaders(friendsCol).contains(enumSchema));
    assertTrue(tlb.getRegisteredWriters(questProgressCol).contains(unionSchema));
    assertTrue(!tlb.getRegisteredReaders(heroismCol).contains(stringSchema));
    assertTrue(tlb.getRegisteredReaders(heroismCol).contains(intSchema));
    assertTrue(!tlb.getRegisteredReaders(heroismCol).contains(unionSchema));
    assertTrue(tlb.getRegisteredReaders(heroismCol).contains(enumSchema));
  }

  @Test
  public void testSchemaRegistrationAtBadColumns() throws IOException {
    final KijiTableLayout layout = KijiTableLayouts
        .getTableLayout(KijiTableLayouts.SCHEMA_REG_TEST);
    final TableLayoutBuilder tlb = new TableLayoutBuilder(layout.getDesc(), getKiji());
    final Schema.Parser p = new Schema.Parser();
    Schema stringSchema = p.parse("\"string\"");

    // Unqualified group family
    try {
      tlb.withReader(new KijiColumnName("profile"), stringSchema);
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("Table 'schema_reg_test' has no column 'profile'.", nsce.getMessage());
    }

    // Fully qualified map family
    try {
      tlb.withReader(new KijiColumnName("heroism:mordor"), stringSchema);
      fail("An exception should have been thrown.");
    } catch (InvalidLayoutException ile) {
      assertEquals("A fully qualified map-type column name was provided.", ile.getMessage());
    }

    // Nonexistent column
    try {
      tlb.withReader(new KijiColumnName("info:name"), stringSchema);
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("Table 'schema_reg_test' has no column 'info:name'.", nsce.getMessage());
    }

    // FINAL column
    try {
      tlb.withReader(new KijiColumnName("clans"), stringSchema);
      fail("An exception should have been thrown.");
    } catch (InvalidLayoutException ile) {
      assertEquals("Final or non-AVRO column schema cannot be modified.", ile.getMessage());
    }
  }
}
