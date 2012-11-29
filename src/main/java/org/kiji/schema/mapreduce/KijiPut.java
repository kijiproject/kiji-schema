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

import static com.google.common.base.Preconditions.checkState;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Objects;
import org.apache.avro.Schema;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.schema.EntityId;
import org.kiji.schema.HBaseColumnName;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiCellDecoderFactory;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiCellFormat;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.SpecificCellDecoderFactory;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.impl.HBaseEntityId;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * KijiPuts are used to build writes to a Kiji table.
 */
public class KijiPut implements KijiMutation {
  private EntityId mEntityId;
  private String mFamily;
  private String mQualifier;
  private long mTimestamp;
  private KijiCell<?> mCell;

  /**
   * Empty constructor for Writables serialization.
   */
  public KijiPut() { }

  /**
   * Creates a new KijiPut.
   *
   * @param entityId The entityId of the value being written.
   * @param family The family of the column to write to.
   * @param qualifier The qualifier of the column to write to.
   * @param timestamp The timestamp to tag this value with.
   * @param cell The cell to write.
   */
  public KijiPut(EntityId entityId, String family, String qualifier, long timestamp,
      KijiCell<?> cell) {
    mEntityId = entityId;
    mFamily = family;
    mQualifier = qualifier;
    mTimestamp = timestamp;
    mCell = cell;
  }

  /**
   * Creates a new KijiPut.
   *
   * @param entityId The entityId of the value being written.
   * @param family The family of the column to write to.
   * @param qualifier The qualifier of the column to write to.
   * @param cell The cell to write.
   */
  public KijiPut(EntityId entityId, String family, String qualifier, KijiCell<?> cell) {
    this(entityId, family, qualifier, Long.MAX_VALUE, cell);
  }

  /**
   * Builds a HBase Put from this KijiPut.
   *
   * @param translator The ColumnNameTranslator used to convert kiji column names to their HBase
   *     counterparts.
   * @param encoder The KijiCellEncoder used to encode values into the put.
   * @return A HBase Put with the same requested values from the kiji put.
   * @throws IOException if there is an error building the HBase Put.
   */
  public Put toPut(ColumnNameTranslator translator, KijiCellEncoder encoder)
      throws IOException {
    final KijiTableLayout layout = translator.getTableLayout();
    final KijiColumnName column = new KijiColumnName(mFamily, mQualifier);
    final HBaseColumnName hbaseColumn = translator.toHBaseColumnName(column);

    // See if this can be pushed into KijiCellEncoder by adding a new KijiCellFormat for counters.
    if (layout.getCellSchema(column).getType() == SchemaType.COUNTER) {
      // Verify that the cell contains a long.
      checkState(Schema.Type.LONG == mCell.getWriterSchema().getType(),
          "Can't write a non 'long' value to a counter cell");

      final byte[] data = Bytes.toBytes((Long) mCell.getData());
      return new Put(mEntityId.getHBaseRowKey())
          .add(hbaseColumn.getFamily(), hbaseColumn.getQualifier(), mTimestamp, data);
    } else {
      // Handle normal puts.
      final byte[] data = encoder.encode(mCell, layout.getCellFormat(column));
      return new Put(mEntityId.getHBaseRowKey())
          .add(hbaseColumn.getFamily(), hbaseColumn.getQualifier(), mTimestamp, data);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    // EntityId.
    final byte[] bytes = mEntityId.getHBaseRowKey();
    out.writeInt(bytes.length);
    out.write(bytes);

    // Family/Qualifier/Timestamp.
    out.writeUTF(mFamily);
    out.writeUTF(mQualifier);
    out.writeLong(mTimestamp);

    // Avro.
    final KijiCellEncoder encoder = new KijiCellEncoder(null);
    final byte[] cellData = encoder.encode(mCell, KijiCellFormat.NONE);
    out.writeUTF(mCell.getWriterSchema().toString());
    out.writeInt(cellData.length);
    out.write(cellData);
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    // EntityId.
    final byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    mEntityId = new HBaseEntityId(bytes);

    // Family/Qualifier/Timestamp.
    mFamily = in.readUTF();
    mQualifier = in.readUTF();
    mTimestamp = in.readLong();

    // Avro.
    final Schema schema = new Schema.Parser().parse(in.readUTF());
    final KijiCellDecoderFactory decoderFactory = new SpecificCellDecoderFactory(null);
    final KijiCellDecoder<?> decoder = decoderFactory.create(schema, KijiCellFormat.NONE);
    final byte[] cellData = new byte[in.readInt()];
    in.readFully(cellData);
    mCell = decoder.decode(cellData, null);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (null == other || !(other instanceof KijiPut)) {
      return false;
    }
    final KijiPut o = (KijiPut) other;
    return new EqualsBuilder()
        .append(mEntityId, o.mEntityId)
        .append(mFamily, o.mFamily)
        .append(mQualifier, o.mQualifier)
        .append(mTimestamp, o.mTimestamp)
        .append(mCell, o.mCell)
        .isEquals();
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(mEntityId)
        .append(mFamily)
        .append(mQualifier)
        .append(mTimestamp)
        .append(mCell)
        .hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("entityId", mEntityId)
        .add("family", mFamily)
        .add("qualifier", mQualifier)
        .add("timestamp", mTimestamp)
        .add("cell", mCell.toString())
        .toString();
  }
}
