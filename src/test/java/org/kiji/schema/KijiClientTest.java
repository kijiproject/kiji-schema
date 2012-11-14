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

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.impl.InMemoryKiji;

/**
 * Base class for tests that interact with kiji as a client.
 * Provides MetaTable and KijiSchemaTable access.
 */
public class KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(KijiClientTest.class);

  /** An in-memory kiji instance. */
  private Kiji mKiji;
  /** A kiji cell decoder factory. */
  private KijiCellDecoderFactory mCellDecoderFactory;
  /** A kiji cell encoder. */
  private KijiCellEncoder mCellEncoder;

  /**
   * Initializes the in-memory kiji for testing.
   *
   * @throws IOException If there is an error.
   */
  @Before
  public void setupMockKiji() throws IOException {
    mKiji = new InMemoryKiji();
    mCellDecoderFactory = new SpecificCellDecoderFactory(mKiji.getSchemaTable());
    mCellEncoder = new KijiCellEncoder(mKiji.getSchemaTable());
  }

  /**
   * Closes the in-memory kiji instance.
   *
   * @throws IOException If there is an error.
   */
  @After
  public void teardownMockKiji() throws IOException {
    LOG.debug("Closing mock kiji instance");
    IOUtils.closeQuietly(mKiji);
  }

  /**
   * Gets the kiji instance for testing.
   *
   * @return The test kiji instance.
   */
  protected Kiji getKiji() {
    return mKiji;
  }

  /**
   * Gets a kiji cell decoder factory.
   *
   * @return A decoder.
   */
  protected KijiCellDecoderFactory getCellDecoderFactory() {
    return mCellDecoderFactory;
  }

  /**
   * Gets a cell encoder.
   *
   * @return An encoder.
   */
  protected KijiCellEncoder getCellEncoder() {
    return mCellEncoder;
  }

  /**
   * Encodes a string into a kiji cell for test input.
   *
   * @param s The string to encode into a kiji cell.
   * @return An encoded byte array that could be put directly into an HBase cell.
   * @throws IOException If there is an error.
   */
  protected byte[] e(String s) throws IOException {
    return getCellEncoder().encode(
        new KijiCell<CharSequence>(Schema.create(Schema.Type.STRING), s), KijiCellFormat.HASH);
  }
}
