// (c) Copyright 2011 WibiData, Inc.
package com.wibidata.core;

import org.junit.After;
import org.junit.Before;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;

/**
 * An integration test that sets up the Wibi table "foo" before tests.
 */
public class FooTableIntegrationTest extends WibiIntegrationTest {

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
   * @return The Wibi table.
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
    mKiji = Kiji.open(getKijiConfiguration());
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
    if (null != mKijiTable) {
      mKijiTable.close();
    }
    if (null != mKiji) {
      mKiji.close();
    }
  }
}
