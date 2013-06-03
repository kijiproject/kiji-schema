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
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.io.parsing.Symbol.Alternative;
import org.apache.avro.io.parsing.Symbol.ErrorAction;
import org.apache.avro.io.parsing.Symbol.Repeater;

import org.kiji.annotations.ApiAudience;

/**
 * General purpose Avro utilities.
 */
@ApiAudience.Private
public final class AvroUtils {

  /** Utility class cannot be instantiated. */
  private AvroUtils() {
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
   * Validates that the provided reader schema can be used to decode avro data written with the
   * provided writer schema.
   *
   * @param reader schema to validate.
   * @param writer schema to validate.
   * @return a result object identifying any validation errors.
   */
  public static ReaderWriterCompatibilityResult checkReaderWriterCompatibility(
      Schema reader,
      Schema writer) {
    try {
      if (symbolHasErrors(new ResolvingGrammarGenerator().generate(writer, reader))) {
        return new ReaderWriterCompatibilityResult(
            ReaderWriterCompatibility.INCOMPATIBLE,
            reader,
            writer,
            String.format("Cannot use reader schema %s to decode data with writer schema %s.",
                reader.toString(true),
                writer.toString(true)));
      } else {
        return new ReaderWriterCompatibilityResult(
            ReaderWriterCompatibility.COMPATIBLE,
            reader,
            writer,
            "Schemas match");
      }
    } catch (IOException ioe) {
      return new ReaderWriterCompatibilityResult(
          ReaderWriterCompatibility.INCOMPATIBLE,
          reader,
          writer,
          ioe.toString());
    }
  }

  /**
   * Returns true if the provided symbol tree contains any Error symbols, indicating that it may
   * fail for some inputs.
   *
   * Note: This code is borrowed from Scott Carey's patch for AVRO-1315. This code should live in
   *     Avro eventually.
   *
   * @param symbol to check.
   * @return true if the provided symbol tree contains error symbols.
   */
  private static boolean symbolHasErrors(Symbol symbol) {
    switch(symbol.kind) {
      case ALTERNATIVE: {
        final Alternative alternative = (Alternative) symbol;
        return symbolsHaveErrors(alternative.symbols);
      }
      case EXPLICIT_ACTION: {
        return false;
      }
      case IMPLICIT_ACTION: {
        return symbol instanceof ErrorAction;
      }
      case REPEATER: {
        final Repeater repeater = (Repeater) symbol;
        return symbolHasErrors(repeater.end)
            || symbolsHaveErrors(
                Arrays.copyOfRange(repeater.production, 1, repeater.production.length));
      }
      case ROOT: {
        return symbolsHaveErrors(
            Arrays.copyOfRange(symbol.production, 1, symbol.production.length));
      }
      case SEQUENCE: {
        return symbolsHaveErrors(symbol.production);
      }
      case TERMINAL: {
        return false;
      }
      default: {
        throw new RuntimeException("unknown symbol kind: " + symbol.kind);
      }
    }
  }

  /**
   * Returns true if the provided symbol trees contain any Error symbols.
   *
   * Note: This code is borrowed from Scott Carey's patch for AVRO-1315. This code should live in
   *     Avro eventually.
   *
   * @param symbols to check
   * @return trye if the provided symbol trees contain error symbols.
   */
  private static boolean symbolsHaveErrors(Symbol[] symbols) {
    if (null != symbols) {
      for (Symbol symbol : symbols) {
        if (symbolHasErrors(symbol)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Identifies the type of a schema validation result.
   */
  public static enum ReaderWriterCompatibility {
    COMPATIBLE,
    INCOMPATIBLE
  }

  /**
   * Provides information about the compatibility of two Avro schemas.
   */
  public static final class ReaderWriterCompatibilityResult {
    /** The type of this result. */
    private final ReaderWriterCompatibility mType;

    /** Validated reader schema. */
    private final Schema mReader;

    /** Validated writer schema. */
    private final Schema mWriter;

    /** Human readable description of this result. */
    private final String mDescription;

    /**
     * Constructs a new result.
     *
     * @param type of the schema validation result.
     * @param reader schema that was validated.
     * @param writer schema that was validated.
     * @param description of this validation result.
     */
    public ReaderWriterCompatibilityResult(
        ReaderWriterCompatibility type,
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
    public ReaderWriterCompatibility getType() {
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
      if (null != other && other instanceof ReaderWriterCompatibilityResult) {
        final ReaderWriterCompatibilityResult result = (ReaderWriterCompatibilityResult) other;
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
