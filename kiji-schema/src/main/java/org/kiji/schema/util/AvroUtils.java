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

package org.kiji.schema.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.avro.AvroSchema;
import org.kiji.schema.layout.AvroSchemaResolver;
import org.kiji.schema.layout.SchemaTableAvroResolver;

/**
 * General purpose Avro utilities.
 */
@ApiAudience.Private
public final class AvroUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AvroUtils.class);

  /** Utility class cannot be instantiated. */
  private AvroUtils() {
  }

  /**
   * Compares two AvroSchema objects for equality within the context of the given SchemaTable.
   *
   * @param schemaTable SchemaTable with which to resolve schema UIDs.
   * @param first one AvroSchema object to compare for equality.
   * @param second another AvroSchema object to compare for equality.
   * @return whether the two objects represent the same Schema.
   * @throws IOException in case of an error reading from the SchemaTable.
   */
  public static boolean avroSchemaEquals(
      final KijiSchemaTable schemaTable,
      final AvroSchema first,
      final AvroSchema second
  ) throws IOException {
    final AvroSchemaResolver resolver = new SchemaTableAvroResolver(schemaTable);

    return Objects.equal(resolver.apply(first), resolver.apply(second));
  }

  /**
   * Check whether a collection of AvroSchema objects contains a given AvroSchema element, resolving
   * UIDs using the given KijiSchemaTable.
   *
   * @param schemaTable KijiSchemaTable with which to resolve schema UIDs.
   * @param schemaCollection collection of AvroSchemas to check for the presence of the given
   *     element.
   * @param element AvroSchema for whose presence to check in schemaCollection.
   * @return whether schemaCollection contains element after resolving UIDs using schemaTable.
   * @throws IOException in case of an error reading from the schema table.
   */
  public static boolean avroSchemaCollectionContains(
      final KijiSchemaTable schemaTable,
      final Collection<AvroSchema> schemaCollection,
      final AvroSchema element
  ) throws IOException {
    for (AvroSchema schema : schemaCollection) {
      if (avroSchemaEquals(schemaTable, schema, element)) {
        return true;
      }
    }
    // If none match, return false.
    return false;
  }

  /**
   * Reports whether the given schema is an optional type (ie. a union { null, Type }).
   *
   * @param schema The schema to test.
   * @return the optional type, if the specified schema describes an optional type, null otherwise.
   */
  public static Schema getOptionalType(Schema schema) {
    Preconditions.checkArgument(schema.getType() == Schema.Type.UNION);
    final List<Schema> types = schema.getTypes();
    if (types.size() != 2) {
      return null;
    }
    if (types.get(0).getType() == Schema.Type.NULL) {
      return types.get(1);
    } else if (types.get(1).getType() == Schema.Type.NULL) {
      return types.get(0);
    } else {
      return null;
    }
  }

  /**
   * Validates that the provided reader schemas can be used to decode data written with the provided
   * writer schema.
   *
   * @param readers that must be able to be used to decode data encoded with the provided writer
   *     schema.
   * @param writer schema to check.
   * @return a list of compatibility results.
   */
  public static SchemaSetCompatibility checkWriterCompatibility(
      Iterator<Schema> readers,
      Schema writer) {
    final List<SchemaPairCompatibility> results = Lists.newArrayList();
    while (readers.hasNext()) {
      // Check compatibility between each reader/writer pair.
      results.add(checkReaderWriterCompatibility(readers.next(), writer));
    }
    return new SchemaSetCompatibility(results);
  }

  /**
   * Validates that the provided reader schema can read data written with the provided writer
   * schemas.
   *
   * @param reader schema to check.
   * @param writers that must be compatible with the provided reader schema.
   * @return a list of compatibility results.
   */
  public static SchemaSetCompatibility checkReaderCompatibility(
      Schema reader,
      Iterator<Schema> writers) {
    final List<SchemaPairCompatibility> results = Lists.newArrayList();
    while (writers.hasNext()) {
      // Check compatibility between each reader/writer pair.
      results.add(checkReaderWriterCompatibility(reader, writers.next()));
    }
    return new SchemaSetCompatibility(results);
  }

  /** Message to annotate reader/writer schema pairs that are compatible. */
  public static final String READER_WRITER_COMPATIBLE_MESSAGE =
      "Reader schema can always successfully decode data written using the writer schema.";

  /**
   * Validates that the provided reader schema can be used to decode avro data written with the
   * provided writer schema.
   *
   * @param reader schema to check.
   * @param writer schema to check.
   * @return a result object identifying any compatibility errors.
   */
  public static SchemaPairCompatibility checkReaderWriterCompatibility(
      final Schema reader,
      final Schema writer
  ) {
    final SchemaCompatibilityType compatibility =
        new ReaderWriterCompatiblityChecker()
            .getCompatibility(reader, writer);

    final String message;
    switch (compatibility) {
      case INCOMPATIBLE: {
        message = String.format(
            "Data encoded using writer schema:%n%s%n"
            + "will or may fail to decode using reader schema:%n%s%n",
            writer.toString(true),
            reader.toString(true));
        break;
      }
      case COMPATIBLE: {
        message = READER_WRITER_COMPATIBLE_MESSAGE;
        break;
      }
      default: throw new InternalKijiError("Unknown compatibility: " + compatibility);
    }

    return new SchemaPairCompatibility(
        compatibility,
        reader,
        writer,
        message);
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Tests the equality of two Avro named schemas.
   *
   * <p> Matching includes reader name aliases. </p>
   *
   * @param reader Named reader schema.
   * @param writer Named writer schema.
   * @return whether the names of the named schemas match or not.
   */
  public static boolean schemaNameEquals(final Schema reader, final Schema writer) {
    final String writerFullName = writer.getFullName();
    if (Objects.equal(reader.getFullName(), writerFullName)) {
      return true;
    }
    // Apply reader aliases:
    if (reader.getAliases().contains(writerFullName)) {
      return true;
    }
    return false;
  }

  /**
   * Identifies the writer field that corresponds to the specified reader field.
   *
   * <p> Matching includes reader name aliases. </p>
   *
   * @param writerSchema Schema of the record where to look for the writer field.
   * @param readerField Reader field to identify the corresponding writer field of.
   * @return the writer field, if any does correspond, or None.
   */
  public static Field lookupWriterField(final Schema writerSchema, final Field readerField) {
    Preconditions.checkArgument(writerSchema.getType() == Type.RECORD);
    final List<Field> writerFields = Lists.newArrayList();
    final Field direct = writerSchema.getField(readerField.name());
    if (direct != null) {
      writerFields.add(direct);
    }
    for (final String readerFieldAliasName : readerField.aliases()) {
      final Field writerField = writerSchema.getField(readerFieldAliasName);
      if (writerField != null) {
        writerFields.add(writerField);
      }
    }
    switch (writerFields.size()) {
      case 0: return null;
      case 1: return writerFields.get(0);
      default: {
        // TODO Report this properly to the user.
        LOG.error("Reader record field {} matches multiple fields in the writer record: {}",
            readerField, Joiner.on(",").join(writerFields));
        // Pick the first writer field that matches:
        return writerFields.get(0);
      }
    }
  }

  /**
   * Reader/writer schema pair that can be used as a key in a hash map.
   *
   * This reader/writer pair differentiates Schema objects based on their system hash code.
   */
  private static final class ReaderWriter {
    private final Schema mReader;
    private final Schema mWriter;

    /**
     * Initializes a new reader/writer pair.
     *
     * @param reader Reader schema.
     * @param writer Writer schema.
     */
    public ReaderWriter(final Schema reader, final Schema writer) {
      mReader = reader;
      mWriter = writer;
    }

    /**
     * Returns the reader schema in this pair.
     * @return the reader schema in this pair.
     */
    public Schema getReader() {
      return mReader;
    }

    /**
     * Returns the writer schema in this pair.
     * @return the writer schema in this pair.
     */
    public Schema getWriter() {
      return mWriter;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return System.identityHashCode(mReader) ^ System.identityHashCode(mWriter);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ReaderWriter)) {
        return false;
      }
      final ReaderWriter that = (ReaderWriter) obj;
      // Use pointer comparison here:
      return (this.mReader == that.mReader)
          && (this.mWriter == that.mWriter);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(ReaderWriter.class)
          .add("reader", mReader)
          .add("writer", mWriter)
          .toString();
    }
  }

  /**
   * Determines the compatibility of a reader/writer schema pair.
   *
   * <p> Provides memoization to handle recursive schemas. </p>
   */
  private static final class ReaderWriterCompatiblityChecker {
    private final Map<ReaderWriter, SchemaCompatibilityType> mMemoizeMap = Maps.newHashMap();

    /**
     * Reports the compatibility of a reader/writer schema pair.
     *
     * <p> Memoizes the compatibility results. </p>
     *
     * @param reader Reader schema to test.
     * @param writer Writer schema to test.
     * @return the compatibility of the reader/writer schema pair.
     */
    public SchemaCompatibilityType getCompatibility(
        final Schema reader,
        final Schema writer
    ) {
      LOG.debug("Checking compatibility of reader {} with writer {}", reader, writer);
      final ReaderWriter pair = new ReaderWriter(reader, writer);
      final SchemaCompatibilityType existing = mMemoizeMap.get(pair);
      if (existing != null) {
        if (existing == SchemaCompatibilityType.RECURSION_IN_PROGRESS) {
          // Break the recursion here.
          // schemas are compatible unless proven incompatible:
          return SchemaCompatibilityType.COMPATIBLE;
        }
        return existing;
      }
      // Mark this reader/writer pair as "in progress":
      mMemoizeMap.put(pair, SchemaCompatibilityType.RECURSION_IN_PROGRESS);
      final SchemaCompatibilityType calculated = calculateCompatibility(reader, writer);
      mMemoizeMap.put(pair, calculated);
      return calculated;
    }

    /**
     * Calculates the compatibility of a reader/writer schema pair.
     *
     * <p>
     * Relies on external memoization performed by {@link #getCompatibility(Schema, Schema)}.
     * </p>
     *
     * @param reader Reader schema to test.
     * @param writer Writer schema to test.
     * @return the compatibility of the reader/writer schema pair.
     */
    private SchemaCompatibilityType calculateCompatibility(
        final Schema reader,
        final Schema writer
    ) {
      Preconditions.checkNotNull(reader);
      Preconditions.checkNotNull(writer);

      if (reader.getType() == writer.getType()) {
        switch (reader.getType()) {
          case NULL:
          case BOOLEAN:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case BYTES:
          case STRING: {
            return SchemaCompatibilityType.COMPATIBLE;
          }
          case ARRAY: {
            return getCompatibility(reader.getElementType(), writer.getElementType());
          }
          case MAP: {
            return getCompatibility(reader.getValueType(), writer.getValueType());
          }
          case FIXED: {
            // fixed size and name must match:
            if (!schemaNameEquals(reader, writer)) {
              return SchemaCompatibilityType.INCOMPATIBLE;
            }
            if (reader.getFixedSize() != writer.getFixedSize()) {
              return SchemaCompatibilityType.INCOMPATIBLE;
            }
            return SchemaCompatibilityType.COMPATIBLE;
          }
          case ENUM: {
            // enum names must match:
            if (!schemaNameEquals(reader, writer)) {
              return SchemaCompatibilityType.INCOMPATIBLE;
            }
            // reader symbols must contain all writer symbols:
            final Set<String> symbols = Sets.newHashSet(writer.getEnumSymbols());
            symbols.removeAll(reader.getEnumSymbols());
            // TODO: Report a human-readable error.
            // if (!symbols.isEmpty()) {
            // }
            return symbols.isEmpty()
                ? SchemaCompatibilityType.COMPATIBLE
                : SchemaCompatibilityType.INCOMPATIBLE;
          }
          case RECORD: {
            // record names must match:
            if (!schemaNameEquals(reader, writer)) {
              return SchemaCompatibilityType.INCOMPATIBLE;
            }

            // Check that each field in the reader record can be populated from the writer record:
            for (final Field readerField : reader.getFields()) {
              final Field writerField = lookupWriterField(writer, readerField);
              if (writerField == null) {
                // Reader field does not correspond to any field in the writer record schema,
                // reader field must have a default value.
                if (readerField.defaultValue() == null) {
                  // reader field has no default value
                  return SchemaCompatibilityType.INCOMPATIBLE;
                }
              } else {
                if (getCompatibility(readerField.schema(), writerField.schema())
                    == SchemaCompatibilityType.INCOMPATIBLE) {
                  return SchemaCompatibilityType.INCOMPATIBLE;
                }
              }
            }

            // All fields in the reader record can be populated from the writer record:
            return SchemaCompatibilityType.COMPATIBLE;
          }
          case UNION: {
            // Check that each individual branch of the writer union can be decoded:
            for (final Schema writerBranch : writer.getTypes()) {
              if (getCompatibility(reader, writerBranch) == SchemaCompatibilityType.INCOMPATIBLE) {
                return SchemaCompatibilityType.INCOMPATIBLE;
              }
            }
            // Each schema in the writer union can be decoded with the reader:
            return SchemaCompatibilityType.COMPATIBLE;
          }

          default: {
            throw new InternalKijiError("Unknown schema type: " + reader.getType());
          }
        }

      } else {
        // Reader and writer have different schema types:

        // Handle the corner case where writer is a union of a singleton branch: { X } === X
        if ((writer.getType() == Schema.Type.UNION)
            && writer.getTypes().size() == 1) {
          return getCompatibility(reader, writer.getTypes().get(0));
        }

        switch (reader.getType()) {
          case NULL: return SchemaCompatibilityType.INCOMPATIBLE;
          case BOOLEAN: return SchemaCompatibilityType.INCOMPATIBLE;
          case INT: return SchemaCompatibilityType.INCOMPATIBLE;
          case LONG: {
            return (writer.getType() == Type.INT)
                ? SchemaCompatibilityType.COMPATIBLE
                : SchemaCompatibilityType.INCOMPATIBLE;
          }
          case FLOAT: {
            return ((writer.getType() == Type.INT)
                || (writer.getType() == Type.LONG))
                ? SchemaCompatibilityType.COMPATIBLE
                : SchemaCompatibilityType.INCOMPATIBLE;

          }
          case DOUBLE: {
            return ((writer.getType() == Type.INT)
                || (writer.getType() == Type.LONG)
                || (writer.getType() == Type.FLOAT))
                ? SchemaCompatibilityType.COMPATIBLE
                : SchemaCompatibilityType.INCOMPATIBLE;
          }
          case BYTES: return SchemaCompatibilityType.INCOMPATIBLE;
          case STRING: return SchemaCompatibilityType.INCOMPATIBLE;
          case ARRAY: return SchemaCompatibilityType.INCOMPATIBLE;
          case MAP: return SchemaCompatibilityType.INCOMPATIBLE;
          case FIXED: return SchemaCompatibilityType.INCOMPATIBLE;
          case ENUM: return SchemaCompatibilityType.INCOMPATIBLE;
          case RECORD: return SchemaCompatibilityType.INCOMPATIBLE;
          case UNION: {
            for (final Schema readerBranch : reader.getTypes()) {
              if (getCompatibility(readerBranch, writer) == SchemaCompatibilityType.COMPATIBLE) {
                return SchemaCompatibilityType.COMPATIBLE;
              }
            }
            // No branch in the reader union has been found compatible with the writer schema:
            return SchemaCompatibilityType.INCOMPATIBLE;
          }

          default: {
            throw new InternalKijiError("Unknown schema type: " + reader.getType());
          }
        }
      }
    }
  }

  /**
   * Identifies the type of a schema compatibility result.
   */
  public static enum SchemaCompatibilityType {
    COMPATIBLE,
    INCOMPATIBLE,

    /** Used internally to tag a reader/writer schema pair and prevent recursion. */
    RECURSION_IN_PROGRESS;
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Provides information about the compatibility of a single schema with a set of schemas.
   */
  public static final class SchemaSetCompatibility {
    /** Whether or not the schemas are compatible. */
    private final SchemaCompatibilityType mType;

    /** The compatibilities of each schema pair. */
    private final List<SchemaPairCompatibility> mCauses;

    /**
     * Constructs a new instance.
     *
     * @param causes identifying the compatibilities of each schema pair.
     */
    public SchemaSetCompatibility(List<SchemaPairCompatibility> causes) {
      SchemaCompatibilityType isCompatible = SchemaCompatibilityType.COMPATIBLE;
      for (SchemaPairCompatibility compatibility : causes) {
        if (compatibility.getType() == SchemaCompatibilityType.INCOMPATIBLE) {
          isCompatible = SchemaCompatibilityType.INCOMPATIBLE;
          break;
        }
      }

      mType = isCompatible;
      mCauses = causes;
    }

    /**
     * Whether or not the schemas are compatible.
     *
     * @return whether or not the schemas are compatible.
     */
    public SchemaCompatibilityType getType() {
      return mType;
    }

    /**
     * Returns the compatibility of each schema pair.
     *
     * @return the compatibility of each schema pair.
     */
    public List<SchemaPairCompatibility> getCauses() {
      return mCauses;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(SchemaSetCompatibility.class)
          .add("type", mType)
          .add("cause", mCauses)
          .toString();
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Provides information about the compatibility of a single reader and writer schema pair.
   *
   * Note: This class represents a one-way relationship from the reader to the writer schema.
   */
  public static final class SchemaPairCompatibility {
    /** The type of this result. */
    private final SchemaCompatibilityType mType;

    /** Validated reader schema. */
    private final Schema mReader;

    /** Validated writer schema. */
    private final Schema mWriter;

    /** Human readable description of this result. */
    private final String mDescription;

    /**
     * Constructs a new instance.
     *
     * @param type of the schema compatibility.
     * @param reader schema that was validated.
     * @param writer schema that was validated.
     * @param description of this compatibility result.
     */
    public SchemaPairCompatibility(
        SchemaCompatibilityType type,
        Schema reader,
        Schema writer,
        String description) {
      mType = type;
      mReader = reader;
      mWriter = writer;
      mDescription = description;
    }

    /**
     * Gets the type of this result.
     *
     * @return the type of this result.
     */
    public SchemaCompatibilityType getType() {
      return mType;
    }

    /**
     * Gets the reader schema that was validated.
     *
     * @return reader schema that was validated.
     */
    public Schema getReader() {
      return mReader;
    }

    /**
     * Gets the writer schema that was validated.
     *
     * @return writer schema that was validated.
     */
    public Schema getWriter() {
      return mWriter;
    }

    /**
     * Gets a human readable description of this validation result.
     *
     * @return a human readable description of this validation result.
     */
    public String getDescription() {
      return mDescription;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(this.getClass())
          .add("type", mType)
          .add("readerSchema", mReader)
          .add("writerSchema", mWriter)
          .add("description", mDescription)
          .toString();
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object other) {
      if ((null != other) && (other instanceof SchemaPairCompatibility)) {
        final SchemaPairCompatibility result = (SchemaPairCompatibility) other;
        return Objects.equal(result.mType, mType)
            && Objects.equal(result.mReader, mReader)
            && Objects.equal(result.mWriter, mWriter)
            && Objects.equal(result.mDescription, mDescription);
      } else {
        return false;
      }
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hashCode(mType, mReader, mWriter, mDescription);
    }
  }
}
