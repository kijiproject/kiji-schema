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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.google.common.base.Objects;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Specification of read properties applied when decoding cells from a column in a Kiji table.
 *
 * <p>
 *   This class provides several factory methods to specify read properties applied when decoding
 *   a column in a Kiji table.
 * </p>
 *
 * <p>
 *   Note: this object assumes that Avro Schema objects are never mutated.
 *   In particular, one should never modify the properties of a Schema object
 *   (eg. using {@link Schema#addProp(String, String)} after it has been constructed.
 * </p>
 *
 * <p>
 *   ColumnReaderSpec Implements {@link Serializable} because
 *   {@link org.kiji.schema.KijiDataRequest} requires it. Do not rely on Java serialization, it will
 *   eventually be replaced by Avro, see SCHEMA-589.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class ColumnReaderSpec
    implements Serializable {

  /** Serialization version. */
  public static final long serialVersionUID = 1L;

  /** Master encoding of the column's content. */
  public static enum Encoding {
    /** Cell content is a raw byte array. */
    RAW_BYTES,

    /** Cell content is encoded using Avro.  */
    AVRO,

    /** Cell content is encoded using Protocol buffers. */
    PROTOBUF,

    /** Cell content is an HBase counter. */
    COUNTER,
  }

  /** For Avro encoded columns, specify which reader schema to apply when decoding cells. */
  public static enum AvroReaderSchemaType {
    /** Decode cells using the default reader schema specified in the table layout. */
    DEFAULT,

    /** Decode cells using the explicitly declared reader schema. */
    EXPLICIT,

    /** Decode cells using the writer schema used at encoding-time. */
    WRITER,
  }

  /** For Avro encoded columns, specifies the type of decoder to use: generic or specific. */
  public static enum AvroDecoderType {
    /** Decode Avro data using the Avro generic API. */
    GENERIC,

    /** Decode Avro data using the Avro specific API. */
    SPECIFIC,
  }

  private static final ColumnReaderSpec COUNTER_READER_SPEC = new ColumnReaderSpec(
      Encoding.COUNTER,
      /* avroReaderSchemaType = */ null,
      /* avroDecoderType = */ null,
      /* avroReaderSchema = */ null,
      /* avroReaderSchemaClass = */ null
  );

  private static final ColumnReaderSpec BYTES_READER_SPEC = new ColumnReaderSpec(
      Encoding.RAW_BYTES,
      /* avroReaderSchemaType = */ null,
      /* avroDecoderType = */ null,
      /* avroReaderSchema = */ null,
      /* avroReaderSchemaClass = */ null
  );

  private static final ColumnReaderSpec PROTOBUF_READER_SPEC = new ColumnReaderSpec(
      Encoding.PROTOBUF,
      /* avroReaderSchemaType = */ null,
      /* avroDecoderType = */ null,
      /* avroReaderSchema = */ null,
      /* avroReaderSchemaClass = */ null
  );

  /**
   * Returns a reader specification for a column containing an HBase counter.
   * @return a reader specification for a column containing an HBase counter.
   */
  public static ColumnReaderSpec counter() {
    return COUNTER_READER_SPEC;
  }

  /**
   * Returns a reader specification for a column containing raw bytes.
   * @return a reader specification for a column containing raw bytes.
   */
  public static ColumnReaderSpec bytes() {
    return BYTES_READER_SPEC;
  }

  /**
   * Returns a reader specification for a column encoded using protocol buffers
   * and decoded using the generated class specified in the table layout.
   *
   * @return a reader specification for a column encoded using protocol buffers
   *     and decoded using the generated class specified in the table layout.
   */
  public static ColumnReaderSpec protobuf() {
    return PROTOBUF_READER_SPEC;
  }

  /**
   * Returns a reader specification for a column encoded using Avro,
   * and decoded with the generic API using the writer schema.
   *
   * @return a reader specification for a column encoded using Avro,
   *     and decoded with the generic API using the writer schema.
   */
  public static ColumnReaderSpec avroWriterSchemaGeneric() {
    return new ColumnReaderSpec(
        Encoding.AVRO,
        AvroReaderSchemaType.WRITER,
        AvroDecoderType.GENERIC,
        /* avroReaderSchema = */ null,
        /* avroReaderSchemaClass = */ null
    );
  }

  /**
   * Returns a reader specification for a column encoded using Avro, and decoded with the generic
   * API using the default Avro reader schema specified in the table layout.
   *
   * @return a reader specification for a column encoded using Avro, and decoded with the generic
   *     API using the default Avro reader schema specified in the table layout.
   */
  public static ColumnReaderSpec avroDefaultReaderSchemaGeneric() {
    return new ColumnReaderSpec(
        Encoding.AVRO,
        AvroReaderSchemaType.DEFAULT,
        AvroDecoderType.GENERIC,
        /* avroReaderSchema = */ null,
        /* avroReaderSchemaClass = */ null
    );
  }

  /**
   * Returns a reader specification for a column encoded using Avro,
   * and decoded with the specific API using a given SpecificRecord generated class.
   *
   * @param readerSchemaClass Class of the specific record to decode.
   * @return a reader specification for a column encoded using Avro,
   *     and decoded with the specific API using the given SpecificRecord generated class.
   * @throws InvalidLayoutException if the specified class is not a valid Avro container class.
   */
  public static ColumnReaderSpec avroReaderSchemaSpecific(
      final Class<? extends SpecificRecord> readerSchemaClass
  ) throws InvalidLayoutException {
    final Schema readerSchema = CellSpec.getSchemaFromClass(readerSchemaClass);
    return new ColumnReaderSpec(
        Encoding.AVRO,
        AvroReaderSchemaType.EXPLICIT,
        AvroDecoderType.SPECIFIC,
        /* avroReaderSchema = */ readerSchema,
        /* avroReaderSchemaClass = */ readerSchemaClass
    );
  }

  /**
   * Returns a reader specification for a column encoded using Avro,
   * and decoded with the generic API using a given Avro schema.
   *
   * @param readerSchema Avro reader schema used to decode cells.
   * @return a reader specification for a column encoded using Avro,
   *     and decoded with the generic API using the specified Avro schema.
   */
  public static ColumnReaderSpec avroReaderSchemaGeneric(
      final Schema readerSchema
  ) {
    return new ColumnReaderSpec(
        Encoding.AVRO,
        AvroReaderSchemaType.EXPLICIT,
        AvroDecoderType.GENERIC,
        /* avroReaderSchema = */ readerSchema,
        /* avroReaderSchemaClass = */ null
    );
  }

  // -----------------------------------------------------------------------------------------------

  /** Master encoding of the column. */
  private final Encoding mEncoding;

  /**
   * Avro specific: determines which Avro reader schema to apply when decoding a cell.
   * Null otherwise.
   */
  private final AvroReaderSchemaType mAvroReaderSchemaType;

  /** Type of Avro decoder to use: specific or generic. */
  private final AvroDecoderType mAvroDecoderType;

  /**
   * Avro reader schema to use. This field is mutable so that it can be manually set during
   * {@link #readObject(java.io.ObjectInputStream)}, it should not be mutated.
   */
  private transient Schema mAvroReaderSchema;

  /** Avro reader schema class to use. */
  private final Class<? extends SpecificRecord> mAvroReaderSchemaClass;

  /**
   * Constructs a new column reader specification.
   *
   * @param encoding Master encoding of the column.
   * @param avroReaderSchemaType Which Avro schema to use as the reader schema.
   * @param avroDecoderType Which decoder to use (generic vs specific).
   * @param avroReaderSchema Explicit reader schema to use.
   * @param avroReaderSchemaClass Explicit specific record class to use.
   */
  private ColumnReaderSpec(
      final Encoding encoding,
      final AvroReaderSchemaType avroReaderSchemaType,
      final AvroDecoderType avroDecoderType,
      final Schema avroReaderSchema,
      final Class<? extends SpecificRecord> avroReaderSchemaClass
  ) {
    mEncoding = encoding;
    mAvroReaderSchemaType = avroReaderSchemaType;
    mAvroDecoderType = avroDecoderType;
    mAvroReaderSchema = avroReaderSchema;
    mAvroReaderSchemaClass = avroReaderSchemaClass;
  }

  /**
   * Returns the master encoding of the column (bytes, counter, Avro or Protobuf).
   * @return the master encoding of the column (bytes, counter, Avro or Protobuf).
   */
  public Encoding getEncoding() {
    return mEncoding;
  }

  /**
   * Returns the Avro reader schema type (default, explicit or writer).
   * @return the Avro reader schema type (default, explicit or writer).
   *     Null if the master encoding is not Avro.
   */
  public AvroReaderSchemaType getAvroReaderSchemaType() {
    return mAvroReaderSchemaType;
  }

  /**
   * Returns the Avro decoder type (generic or specific).
   * @return the Avro decoder type (generic or specific).
   *     Null if the master encoding is not Avro.
   */
  public AvroDecoderType getAvroDecoderType() {
    return mAvroDecoderType;
  }

  /**
   * Returns the explicit Avro reader schema to use to decode the column.
   * @return the explicit Avro reader schema to use to decode the column.
   *     Null if the master encoding is not Avro, or if the reader schema type is not explicit.
   */
  public Schema getAvroReaderSchema() {
    return mAvroReaderSchema;
  }

  /**
   * Returns the class of the SpecificRecord to use to decode the column.
   * @return the class of the SpecificRecord to use to decode the column.
   *     Null if the master encoding is not Avro,
   *     or if the reader schema type is not explicit,
   *     or if using the generic API.
   */
  public Class<? extends SpecificRecord> getAvroReaderSchemaClass() {
    return mAvroReaderSchemaClass;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(
        mEncoding,
        mAvroReaderSchemaType,
        mAvroDecoderType,
        mAvroReaderSchema,
        mAvroReaderSchemaClass
    );
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object object) {
    if (!(object instanceof ColumnReaderSpec)) {
      return false;
    }
    final ColumnReaderSpec that = (ColumnReaderSpec) object;
    return (this.mEncoding == that.mEncoding)
        && (this.mAvroReaderSchemaType == that.mAvroReaderSchemaType)
        && (this.mAvroDecoderType == that.mAvroDecoderType)
        && Objects.equal(this.mAvroReaderSchema, that.mAvroReaderSchema)
        && Objects.equal(this.mAvroReaderSchemaClass, that.mAvroReaderSchemaClass);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(ColumnReaderSpec.class)
        .add("encoding", mEncoding)
        .add("avro_reader_schema_type", mAvroReaderSchemaType)
        .add("avro_decoder_type", mAvroDecoderType)
        .add("avro_reader_schema", mAvroReaderSchema)
        .add("avro_reader_schema_class", mAvroReaderSchemaClass)
        .toString();
  }

  /**
   * Deserialize an instance of this object using Java serialization.
   *
   * @param inStream input stream from which to deserialize this object.
   * @throws ClassNotFoundException in case this class is not found.
   * @throws IOException in case of an error reading from the input stream.
   */
  private void readObject(
      final ObjectInputStream inStream
  ) throws ClassNotFoundException, IOException {
    inStream.defaultReadObject();
    final String avroSchemaString = inStream.readUTF();
    if (avroSchemaString.isEmpty()) {
      mAvroReaderSchema = null;
    } else {
      mAvroReaderSchema = new Schema.Parser().parse(avroSchemaString);
    }
  }

  /**
   * Serialize this object using Java serialization.
   *
   * @param outStream output stream into which to serialize this object.
   * @throws IOException in case of an error writing this object.
   */
  private void writeObject(
      final ObjectOutputStream outStream
  ) throws IOException {
    outStream.defaultWriteObject();
    outStream.writeUTF((null != mAvroReaderSchema) ? mAvroReaderSchema.toString() : "");
  }
}
