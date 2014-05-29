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

package org.kiji.schema.testutil;

import org.junit.After;
import org.junit.Before;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.util.ResourceUtils;

/**
 * An integration test that sets up the Kiji table "foo" before tests.
 */
public class FooTableIntegrationTest extends AbstractKijiIntegrationTest {

  /** Kiji instance used for this test. */
  private Kiji mKiji;

  /** Kiji table instance connected to test table "foo". */
  private KijiTable mKijiTable;

  /**
   * Gets the Kiji instance used by this test.
   *
   * @return The Kiji instance.
   */
  public Kiji getKiji() {
    return mKiji;
  }

  /**
   * Gets the KijiTable instance configured to use the "foo" table.
   *
   * @return The Kiji table.
   */
  public KijiTable getFooTable() {
    return mKijiTable;
  }
  /**
   * Setup the resources needed by this test.
   *
   * @throws Exception If there is an error.
   */
  @Before
  public void setupFooTable() throws Exception {
    createAndPopulateFooTable();
    mKiji = Kiji.Factory.open(getKijiURI(), getIntegrationHelper().getConf());
    mKijiTable = mKiji.openTable("foo");
  }

  /**
   * Releases the resources needed by this test.
   *
   * @throws Exception If there is an error.
   */
  @After
  public void teardownFooTable() throws Exception {
    deleteFooTable();
    ResourceUtils.releaseOrLog(mKijiTable);
    ResourceUtils.releaseOrLog(mKiji);
  }
}
