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

package org.kiji.schema.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Objects;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.client.Increment;

import org.kiji.schema.EntityId;
import org.kiji.schema.HBaseColumnName;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.impl.RawEntityId;
import org.kiji.schema.layout.ColumnNameTranslator;

/**
 * KijiIncrements are used to increment counters in a Kiji table from within a map-reduce job.
 */
public class KijiIncrement implements KijiMutation {
  private EntityId mEntityId;
  private String mFamily;
  private String mQualifier;
  private long mAmount;

  /**
   * Empty constructor for Writables serialization.
   */
  public KijiIncrement() { }

  /**
   * Creates a new KijiIncrement.
   *
   * @param entityId The entityId of the counter being incremented.
   * @param family The family of the counter being incremented.
   * @param qualifier The qualifier of the counter being incremented.
   * @param amount The amount to increment the counter. Use a negative value here
   *     to perform a decrement
   */
  public KijiIncrement(EntityId entityId, String family, String qualifier, long amount) {
    mEntityId = entityId;
    mFamily = family;
    mQualifier = qualifier;
    mAmount = amount;
  }

  /**
   * Converts this KijiIncrement into an HBase {@link Increment}.
   *
   * @param translator The ColumnNameTranslator used to convert kiji column names to their HBase
   *     counterparts.
   * @return An HBase {@link Increment} built from this KijiIncrement.
   * @throws IOException If there is an error performing the increment.
   */
  public Increment toIncrement(ColumnNameTranslator translator) throws IOException {
    // Translate the Kiji column name to an HBase column name.
    final HBaseColumnName hbaseColumnName = translator.toHBaseColumnName(
        new KijiColumnName(mFamily, mQualifier));

    // Build the HBase increment object.
    final Increment increment = new Increment(mEntityId.getHBaseRowKey());
    increment.addColumn(
        hbaseColumnName.getFamily(),
        hbaseColumnName.getQualifier(),
        mAmount);

    return increment;
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    final byte[] bytes = mEntityId.getHBaseRowKey();
    out.writeInt(bytes.length);
    out.write(bytes);

    // Family/Qualifier/Amount.
    out.writeUTF(mFamily);
    out.writeUTF(mQualifier);
    out.writeLong(mAmount);
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    // EntityId.
    final byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    mEntityId = new RawEntityId(bytes);

    // Family/Qualifier/Timestamp.
    mFamily = in.readUTF();
    mQualifier = in.readUTF();
    mAmount = in.readLong();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (null == other || !(other instanceof KijiIncrement)) {
      return false;
    }
    final KijiIncrement o = (KijiIncrement) other;
    return new EqualsBuilder()
        .append(mEntityId, o.mEntityId)
        .append(mFamily, o.mFamily)
        .append(mQualifier, o.mQualifier)
        .append(mAmount, o.mAmount)
        .isEquals();
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(mEntityId)
        .append(mFamily)
        .append(mQualifier)
        .append(mAmount)
        .hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("entityId", mEntityId)
        .add("family", mFamily)
        .add("qualifier", mQualifier)
        .add("amount", mAmount)
        .toString();
  }
}
