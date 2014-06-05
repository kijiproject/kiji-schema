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

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableAnnotator;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestNotesTool extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestNotesTool.class);

  /** Horizontal ruler to delimit CLI outputs in logs. */
  private static final String RULER =
      "--------------------------------------------------------------------------------";

  /** Output of the CLI tool, as bytes. */
  private ByteArrayOutputStream mToolOutputBytes = new ByteArrayOutputStream();

  /** Output of the CLI tool, as a single string. */
  private String mToolOutputStr;

  /** Output of the CLI tool, as an array of lines. */
  private String[] mToolOutputLines;

  private int runTool(BaseTool tool, String...arguments) throws Exception {
    mToolOutputBytes.reset();
    final PrintStream pstream = new PrintStream(mToolOutputBytes);
    tool.setPrintStream(pstream);
    tool.setConf(getConf());
    try {
      LOG.info("Running tool: '{}' with parameters {}", tool.getName(), arguments);
      return tool.toolMain(Lists.newArrayList(arguments));
    } finally {
      pstream.flush();
      pstream.close();

      mToolOutputStr = Bytes.toString(mToolOutputBytes.toByteArray());
      LOG.info("Captured output for tool: '{}' with parameters {}:\n{}\n{}{}\n",
          tool.getName(), arguments,
          RULER, mToolOutputStr, RULER);
      mToolOutputLines = mToolOutputStr.split("\n");
    }
  }

  //------------------------------------------------------------------------------------------------

  private static final String KEY = "abc";
  private static final String VALUE = "def";
  private static final String KEY2 = "123";
  private static final String VALUE2 = "456";
  private static final String KEY3 = "zyx";
  private static final String VALUE3 = "wvu";
  private static final Map<String, String> KVS =
      ImmutableMap.<String, String>builder().put(KEY, VALUE).put(KEY2, VALUE2).build();
  private static final KijiColumnName INFONAME = KijiColumnName.create("info:name");
  private static final KijiColumnName INFOEMAIL = KijiColumnName.create("info:email");
  private static final String INFO = "info";

  //------------------------------------------------------------------------------------------------

  private KijiTable mTable = null;
  private KijiTableAnnotator mAnnotator = null;

  @Before
  public void setup() throws IOException {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.USER_TABLE));
    mTable = getKiji().openTable("user");
    mAnnotator = mTable.openTableAnnotator();
  }

  @After
  public void cleanup() throws IOException {
    mAnnotator.close();
    mTable.release();
  }

  private KijiURI getInfoNameURI() {
    return KijiURI.newBuilder(mTable.getURI())
        .withColumnNames(Lists.newArrayList(KijiColumnName.create("info:name"))).build();
  }

  private void assertFailure(String... args) throws Exception {
    assertEquals(BaseTool.FAILURE, runTool(new NotesTool(), args));
  }

  private void assertSuccess(String... args) throws Exception {
    assertEquals(BaseTool.SUCCESS, runTool(new NotesTool(), args));
  }

  @Test
  public void testValidateFlags() throws Exception {
    assertFailure("--target=kiji://.env/*^%/table");
    assertTrue(mToolOutputStr.startsWith("Invalid Kiji URI:"));

    assertFailure("--target=kiji://.env/default");
    assertTrue(mToolOutputStr.startsWith("--target URI must specify at least a table, got:"));

    assertFailure("--target=kiji://.env/default/table/info:name,info:email");
    assertEquals("--target URI must include one or zero columns.\n", mToolOutputStr);

    assertFailure("--target=kiji://.env/default/table/info:name", "--do=fake");
    assertEquals("Invalid --do command: 'fake'. Valid commands are set, remove, get\n",
        mToolOutputStr);

    assertFailure("--target=kiji://.env/default/table/info:name", "--do=get", "--in-family=info",
        "--key=key");
    assertEquals("--in-family requires that no columns are specified in --target\n",
        mToolOutputStr);
  }

  @Test
  public void testSetValidatFlags() throws Exception {
    assertFailure("--target=" + getInfoNameURI(), "--do=set", "--prefix");
    assertEquals("Cannot specify --prefix, --partial, or --regex while --do=set\n", mToolOutputStr);

    assertFailure("--target=" + getInfoNameURI(), "--do=set", "--partial");
    assertEquals("Cannot specify --prefix, --partial, or --regex while --do=set\n", mToolOutputStr);

    assertFailure("--target=" + getInfoNameURI(), "--do=set", "--regex");
    assertEquals("Cannot specify --prefix, --partial, or --regex while --do=set\n", mToolOutputStr);

    assertFailure("--target=" + getInfoNameURI(), "--do=set", "--in-family=info");
    assertEquals("Cannot specify a family with --in-family while --do=set\n", mToolOutputStr);

    assertFailure("--target=" + getInfoNameURI(), "--do=set");
    assertEquals("--do=set requires --key and --value OR --map\n", mToolOutputStr);

    assertFailure("--target=" + getInfoNameURI(), "--do=set", "--key=key", "--map=bope");
    assertEquals("--map is mutually exclusive with --key and --value\n", mToolOutputStr);
  }

  @Test
  public void testRemoveValidateFlags() throws Exception {
    assertFailure(
        "--target=" + getInfoNameURI(), "--do=remove", "--prefix", "--partial");
    assertEquals("Cannot specify more than one of --prefix, --partial, and --regex\n",
        mToolOutputStr);

    assertFailure("--target=" + getInfoNameURI(), "--do=remove", "--map=bope");
    assertEquals("Cannot specify --map while --do=remove\n", mToolOutputStr);

    assertFailure("--target=" + getInfoNameURI(), "--do=remove", "--value=bope");
    assertEquals("Cannot specify --value while --do=remove\n", mToolOutputStr);
  }

  @Test
  public void testGetValidateFlags() throws Exception {
    assertFailure("--target=" + getInfoNameURI(), "--do=get", "--prefix", "--partial");
    assertEquals("Cannot specify more than one of --prefix, --partial, and --regex\n",
        mToolOutputStr);

    assertFailure("--target=" + getInfoNameURI(), "--do=get", "--map=bope");
    assertEquals("Cannot specify --map while --do=get\n", mToolOutputStr);

    assertFailure("--target=" + getInfoNameURI(), "--do=get", "--value=bope");
    assertEquals("Cannot specify --value while --do=get\n", mToolOutputStr);
  }

  @Test
  public void testSet() throws Exception {
    assertSuccess(
        "--target=" + getInfoNameURI(),
        "--do=set",
        "--key=" + KEY,
        "--value=" + VALUE);
    assertEquals(VALUE, mAnnotator.getColumnAnnotation(INFONAME, KEY));

    assertSuccess(
        "--target=" + mTable.getURI(),
        "--do=set",
        "--key=" + KEY,
        "--value=" + VALUE);
    assertEquals(VALUE, mAnnotator.getTableAnnotation(KEY));

    assertSuccess(
        "--target=" + getInfoNameURI(),
        "--do=set",
        "--map=" + String.format("{\"%s\":\"%s\"}", KEY2, VALUE2));
    assertEquals(VALUE2, mAnnotator.getColumnAnnotation(INFONAME, KEY2));

    assertSuccess(
        "--target=" + mTable.getURI(),
        "--do=set",
        "--map=" + String.format("{\"%s\":\"%s\"}", KEY2, VALUE2));
    assertEquals(VALUE2, mAnnotator.getTableAnnotation(KEY2));
  }

  @Test
  public void testRemove() throws Exception {
    // exact
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + getInfoNameURI(), "--key=" + KEY, "--do=remove");
    assertNull(mAnnotator.getColumnAnnotation(INFONAME, KEY));

    mAnnotator.setTableAnnotation(KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=" + KEY, "--do=remove");
    assertNull(mAnnotator.getTableAnnotation(KEY));

    mAnnotator.setColumnAnnotation(INFOEMAIL, KEY, VALUE);
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=" + KEY, "--do=remove", "--in-family=info");
    assertTrue(mAnnotator.getColumnAnnotationsInFamily("info", KEY).isEmpty());

    // starts with
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + getInfoNameURI(), "--key=" + KEY.substring(0, 1), "--do=remove",
        "--prefix");
    assertNull(mAnnotator.getColumnAnnotation(INFONAME, KEY));

    mAnnotator.setTableAnnotation(KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=" + KEY.substring(0, 1), "--do=remove",
        "--prefix");
    assertNull(mAnnotator.getTableAnnotation(KEY));

    mAnnotator.setColumnAnnotation(INFOEMAIL, KEY, VALUE);
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=" + KEY.substring(0, 1), "--do=remove",
        "--in-family=info", "--prefix");
    assertTrue(mAnnotator.getColumnAnnotationsInFamily("info", KEY).isEmpty());

    // contains
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + getInfoNameURI(), "--key=" + KEY.substring(1, 2), "--do=remove",
        "--partial");
    assertNull(mAnnotator.getColumnAnnotation(INFONAME, KEY));

    mAnnotator.setTableAnnotation(KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=" + KEY.substring(1, 2), "--do=remove",
        "--partial");
    assertNull(mAnnotator.getTableAnnotation(KEY));

    mAnnotator.setColumnAnnotation(INFOEMAIL, KEY, VALUE);
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=" + KEY.substring(1, 2), "--do=remove",
        "--in-family=info", "--partial");
    assertTrue(mAnnotator.getColumnAnnotationsInFamily("info", KEY).isEmpty());

    // matching
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + getInfoNameURI(), "--key=^[a-z]*$", "--do=remove", "--regex");
    assertNull(mAnnotator.getColumnAnnotation(INFONAME, KEY));

    mAnnotator.setTableAnnotation(KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=^[a-z]*$", "--do=remove", "--regex");
    assertNull(mAnnotator.getTableAnnotation(KEY));

    mAnnotator.setColumnAnnotation(INFOEMAIL, KEY, VALUE);
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=^[a-z]*$", "--do=remove",
        "--in-family=info", "--regex");
    assertTrue(mAnnotator.getColumnAnnotationsInFamily("info", KEY).isEmpty());

    // All
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + getInfoNameURI(), "--do=remove");
    assertNull(mAnnotator.getColumnAnnotation(INFONAME, KEY));

    mAnnotator.setTableAnnotation(KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--do=remove");
    assertNull(mAnnotator.getTableAnnotation(KEY));

    mAnnotator.setColumnAnnotation(INFOEMAIL, KEY, VALUE);
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--do=remove", "--in-family=info");
    assertTrue(mAnnotator.getColumnAnnotationsInFamily("info", KEY).isEmpty());
  }

  @Test
  public void testGet() throws Exception {
    // exact
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + getInfoNameURI(), "--key=" + KEY, "--do=get");
    assertEquals("column: 'info:name'", mToolOutputLines[0]);
    assertEquals("  'abc': 'def'", mToolOutputLines[1]);

    mAnnotator.setTableAnnotation(KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=" + KEY, "--do=get");
    assertEquals("table: 'user'", mToolOutputLines[0]);
    assertEquals("  'abc': 'def'", mToolOutputLines[1]);

    mAnnotator.setColumnAnnotation(INFOEMAIL, KEY, VALUE);
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=" + KEY, "--do=get", "--in-family=info");
    assertEquals("column: 'info:email'", mToolOutputLines[0]);
    assertEquals("  'abc': 'def'", mToolOutputLines[1]);
    assertEquals("column: 'info:name'", mToolOutputLines[2]);
    assertEquals("  'abc': 'def'", mToolOutputLines[3]);

    // starts with
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + getInfoNameURI(), "--key=" + KEY.substring(0, 1), "--do=get",
        "--prefix");
    assertEquals("column: 'info:name'", mToolOutputLines[0]);
    assertEquals("  'abc': 'def'", mToolOutputLines[1]);

    mAnnotator.setTableAnnotation(KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=" + KEY.substring(0, 1), "--do=get",
        "--prefix");
    assertEquals("table: 'user'", mToolOutputLines[0]);
    assertEquals("  'abc': 'def'", mToolOutputLines[1]);

    mAnnotator.setColumnAnnotation(INFOEMAIL, KEY, VALUE);
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=" + KEY.substring(0, 1), "--do=get",
        "--in-family=info", "--prefix");
    assertEquals("column: 'info:email'", mToolOutputLines[0]);
    assertEquals("  'abc': 'def'", mToolOutputLines[1]);
    assertEquals("column: 'info:name'", mToolOutputLines[2]);
    assertEquals("  'abc': 'def'", mToolOutputLines[3]);

    // contains
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + getInfoNameURI(), "--key=" + KEY.substring(1, 2), "--do=get",
        "--partial");
    assertEquals("column: 'info:name'", mToolOutputLines[0]);
    assertEquals("  'abc': 'def'", mToolOutputLines[1]);

    mAnnotator.setTableAnnotation(KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=" + KEY.substring(1, 2), "--do=get",
        "--partial");
    assertEquals("table: 'user'", mToolOutputLines[0]);
    assertEquals("  'abc': 'def'", mToolOutputLines[1]);

    mAnnotator.setColumnAnnotation(INFOEMAIL, KEY, VALUE);
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=" + KEY.substring(1, 2), "--do=get",
        "--in-family=info", "--partial");
    assertEquals("column: 'info:email'", mToolOutputLines[0]);
    assertEquals("  'abc': 'def'", mToolOutputLines[1]);
    assertEquals("column: 'info:name'", mToolOutputLines[2]);
    assertEquals("  'abc': 'def'", mToolOutputLines[3]);

    // matches
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + getInfoNameURI(), "--key=^[a-z]*$", "--do=get", "--regex");
    assertEquals("column: 'info:name'", mToolOutputLines[0]);
    assertEquals("  'abc': 'def'", mToolOutputLines[1]);

    mAnnotator.setTableAnnotation(KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=^[a-z]*$", "--do=get", "--regex");
    assertEquals("table: 'user'", mToolOutputLines[0]);
    assertEquals("  'abc': 'def'", mToolOutputLines[1]);

    mAnnotator.setColumnAnnotation(INFOEMAIL, KEY, VALUE);
    mAnnotator.setColumnAnnotation(INFONAME, KEY, VALUE);
    assertSuccess("--target=" + mTable.getURI(), "--key=^[a-z]*$", "--do=get", "--in-family=info",
        "--regex");
    assertEquals("column: 'info:email'", mToolOutputLines[0]);
    assertEquals("  'abc': 'def'", mToolOutputLines[1]);
    assertEquals("column: 'info:name'", mToolOutputLines[2]);
    assertEquals("  'abc': 'def'", mToolOutputLines[3]);
  }
}
