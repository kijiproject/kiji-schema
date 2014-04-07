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

package org.kiji.schema.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.ToJson;

public class TestCreateTableTool extends KijiToolTest {
  /** Path to a region splits files. */
  public static final String REGION_SPLIT_KEY_FILE = "org/kiji/schema/tools/split-keys.txt";

  /**
   * Writes a table layout as a JSON descriptor in a temporary file.
   *
   * @param layoutDesc Table layout descriptor to write.
   * @return the temporary File where the layout has been written.
   * @throws Exception on error.
   */
  private File getTempLayoutFile(TableLayoutDesc layoutDesc) throws Exception {
    final File layoutFile = File.createTempFile(layoutDesc.getName(), ".json", getLocalTempDir());
    final OutputStream fos = new FileOutputStream(layoutFile);
    try {
      IOUtils.write(ToJson.toJsonString(layoutDesc), fos);
    } finally {
      fos.close();
    }
    return layoutFile;
  }

  private File getTempLayoutFile(KijiTableLayout layout) throws Exception {
    return getTempLayoutFile(layout.getDesc());
  }

  @Test
  public void testCreateHashedTableWithNumRegions() throws Exception {
    final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.FOO_TEST);
    final File layoutFile = getTempLayoutFile(layout);
    final KijiURI tableURI =
        KijiURI.newBuilder(getKiji().getURI()).withTableName(layout.getName()).build();

    assertEquals(BaseTool.SUCCESS, runTool(new CreateTableTool(),
      "--table=" + tableURI,
      "--layout=" + layoutFile,
      "--num-regions=" + 2,
      "--debug"
    ));
    assertEquals(2, mToolOutputLines.length);
    assertTrue(mToolOutputLines[0].startsWith("Parsing table layout: "));
    assertTrue(mToolOutputLines[1].startsWith("Creating Kiji table"));
  }

  @Test
  public void testCreateUnhashedTableWithSplitKeys() throws Exception {
    final KijiTableLayout layout =
        KijiTableLayout.newLayout(KijiTableLayouts.getFooUnhashedTestLayout());
    final File layoutFile = getTempLayoutFile(layout);
    final KijiURI tableURI =
        KijiURI.newBuilder(getKiji().getURI()).withTableName(layout.getName()).build();

    final String splitKeyFile =
        getClass().getClassLoader().getResource(REGION_SPLIT_KEY_FILE).getPath();

    assertEquals(BaseTool.SUCCESS, runTool(new CreateTableTool(),
      "--table=" + tableURI,
      "--layout=" + layoutFile,
      "--split-key-file=file://" + splitKeyFile,
      "--debug"
    ));
    assertEquals(2, mToolOutputLines.length);
    assertTrue(mToolOutputLines[0].startsWith("Parsing table layout: "));
    assertTrue(mToolOutputLines[1].startsWith("Creating Kiji table"));

  }

  @Test
  public void testCreateHashedTableWithSplitKeys() throws Exception {
    final KijiTableLayout layout =
        KijiTableLayouts.getTableLayout(KijiTableLayouts.FOO_TEST_LEGACY);
    final File layoutFile = getTempLayoutFile(layout);
    final KijiURI tableURI =
        KijiURI.newBuilder(getKiji().getURI()).withTableName(layout.getName()).build();

    final String splitKeyFile =
        getClass().getClassLoader().getResource(REGION_SPLIT_KEY_FILE).getPath();

    try {
      runTool(new CreateTableTool(),
        "--table=" + tableURI,
        "--layout=" + layoutFile,
        "--split-key-file=file://" + splitKeyFile
      );
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith(
          "Row key hashing is enabled for the table. Use --num-regions=N instead."));
    }
  }

  @Test
  public void testCreateUnhashedTableWithNumRegions() throws Exception {
    final KijiTableLayout layout =
        KijiTableLayout.newLayout(KijiTableLayouts.getFooUnhashedTestLayout());
    final File layoutFile = getTempLayoutFile(layout);
    final KijiURI tableURI =
        KijiURI.newBuilder(getKiji().getURI()).withTableName(layout.getName()).build();

    try {
      runTool(new CreateTableTool(),
        "--table=" + tableURI,
        "--layout=" + layoutFile,
        "--num-regions=4"
      );
      fail("Should throw InvalidLayoutException");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().startsWith(
          "May not use numRegions > 1 if row key hashing is disabled in the layout"));
    }
  }

  @Test
  public void testCreateTableWithInvalidSchemaClassInLayout() throws Exception {
    final TableLayoutDesc layout = KijiTableLayouts.getLayout(KijiTableLayouts.INVALID_SCHEMA);
    final File layoutFile = getTempLayoutFile(layout);
    final KijiURI tableURI =
        KijiURI.newBuilder(getKiji().getURI()).withTableName(layout.getName()).build();

    try {
      runTool(new CreateTableTool(),
        "--table=" + tableURI,
        "--layout=" + layoutFile
      );
      fail("Should throw InvalidLayoutException");
    } catch (InvalidLayoutException ile) {
      assertTrue(ile.getMessage(), ile.getMessage().startsWith(
          "Invalid cell specification with Avro class type has invalid class name: '\"string\"'."));
    }
  }
}
