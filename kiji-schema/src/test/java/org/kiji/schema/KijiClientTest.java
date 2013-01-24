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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.impl.AvroCellEncoder;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.layout.impl.CellSpec;

/**
 * Base class for tests that interact with kiji as a client.
 * Provides MetaTable and KijiSchemaTable access.
 */
public class KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(KijiClientTest.class);
  private static final AtomicLong INSTANCE_COUNTER = new AtomicLong();

  /** An in-memory kiji instance. */
  private Kiji mKiji;

  /** An in-memory kiji admin instance. */
  private KijiAdmin mKijiAdmin;

  /** The URI of the in-memory kiji instance. */
  private KijiURI mURI;

  /** The configuration object for this kiji instance. */
  private Configuration mConf;

  /** A kiji cell decoder factory. */
  private KijiCellDecoderFactory mCellDecoderFactory;

  /** Cell encoders. */
  private KijiCellEncoder mStringCellEncoder;
  private KijiCellEncoder mIntCellEncoder;

  /**
   * Initializes the in-memory kiji for testing.
   *
   * @throws IOException If there is an error.
   */
  @Before
  public void setupMockKiji() throws IOException {
    try {
      mConf = HBaseConfiguration.create();
      final long id = INSTANCE_COUNTER.getAndIncrement();
      String instanceName = getClass().getSimpleName() + "_test_instance";
      mURI = KijiURI.parse(String.format("kiji://.fake.%d/" + instanceName, id));
      KijiInstaller.install(mURI, mConf);

      mKiji = Kiji.Factory.open(mURI, mConf);
      final HBaseAdmin hbaseAdmin = HBaseFactory.Provider.get()
          .getHBaseAdminFactory(getKijiURI())
          .create(mConf);
      mKijiAdmin = getKiji().getAdmin();
    } catch (KijiURIException kue) {
      throw new IOException(kue);
    } catch (KijiInvalidNameException kine) {
      throw new IOException(kine);
    }
    mCellDecoderFactory = SpecificCellDecoderFactory.get();

    final CellSchema stringCellSchema = CellSchema.newBuilder()
        .setStorage(SchemaStorage.HASH)
        .setType(SchemaType.INLINE)
        .setValue("\"string\"")
        .build();
    final CellSpec stringCellSpec = new CellSpec()
        .setCellSchema(stringCellSchema)
        .setSchemaTable(mKiji.getSchemaTable());
    mStringCellEncoder = new AvroCellEncoder(stringCellSpec);

    final CellSchema intCellSchema = CellSchema.newBuilder()
        .setStorage(SchemaStorage.HASH)
        .setType(SchemaType.INLINE)
        .setValue("\"int\"")
        .build();
    final CellSpec intCellSpec = new CellSpec()
        .setCellSchema(intCellSchema)
        .setSchemaTable(mKiji.getSchemaTable());
    mIntCellEncoder = new AvroCellEncoder(intCellSpec);
  }

  /**
   * Closes the in-memory kiji instance.
   *
   * @throws IOException If there is an error.
   */
  @After
  public void teardownMockKiji() throws IOException {
    LOG.debug("Closing mock kiji instance");
    mKiji.release();
    mKiji = null;
    mKijiAdmin = null;
    mURI = null;
    mConf = null;
  }

  /**
   * Gets the kiji instance for testing.
   *
   * @return the test kiji instance. No need to release.
   */
  protected Kiji getKiji() {
    return mKiji;
  }

  /**
   * Gets the kiji admin instance for testing.
   *
   * @return The test kiji admin instance.
   */
  protected KijiAdmin getKijiAdmin() {
    return mKijiAdmin;
  }

  /**
   * Gets the uri of the kiji instance used for testing.
   *
   * @return The uri of the test kiji instance.
   */
  protected KijiURI getKijiURI() {
    return mURI;
  }

  protected Configuration getConfiguration() {
    return mConf;
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
  @Deprecated
  protected KijiCellEncoder getCellEncoder() {
    return mStringCellEncoder;
  }

  /**
   * Encodes a string into a kiji cell for test input.
   *
   * @param str The string to encode into a kiji cell.
   * @return An encoded byte array that could be put directly into an HBase cell.
   * @throws IOException If there is an error.
   */
  protected byte[] e(String str) throws IOException {
    return mStringCellEncoder.encode(str);
  }

  protected byte[] i(int integer) throws IOException {
    return mIntCellEncoder.encode(integer);
  }

  protected KijiCellEncoder getEncoder(Schema schema)
      throws IOException {
    final CellSchema cellSchema = CellSchema.newBuilder()
        .setStorage(SchemaStorage.HASH)
        .setType(SchemaType.INLINE)
        .setValue(schema.toString())
        .build();
    final CellSpec cellSpec = new CellSpec()
        .setCellSchema(cellSchema)
        .setSchemaTable(mKiji.getSchemaTable());
    return DefaultKijiCellEncoderFactory.get().create(cellSpec);
  }
}
