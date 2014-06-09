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

package org.kiji.schema.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonParser.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.util.ByteArrayFormatter;


/**
 * Utility class providing static methods used by command-line tools.
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class ToolUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ToolUtils.class);

  /** Disable this constructor. */
  private ToolUtils() {}

  /** Prefix to specify an HBase row key from the command-line. */
  public static final String HBASE_ROW_KEY_SPEC_PREFIX = "hbase=";

  /** Optional prefix to specify a Kiji row key from the command-line. */
  public static final String KIJI_ROW_KEY_SPEC_PREFIX = "kiji=";

  /**
   * Parses a command-line flag specifying an entity ID.
   *
   * <li> HBase row key specifications must be prefixed with <code>"hbase=..."</code>
   * <li> Kiji row key specifications may be explicitly prefixed with <code>"kiji=..."</code> if
   *     necessary. The prefix is not always necessary.
   *
   * @param entityFlag Command-line flag specifying an entity ID.
   * @param layout Layout of the table describing the entity ID format.
   * @return the entity ID as specified in the flags.
   * @throws IOException on I/O error.
   */
  public static EntityId createEntityIdFromUserInputs(String entityFlag, KijiTableLayout layout)
      throws IOException {
    Preconditions.checkNotNull(entityFlag);

    final EntityIdFactory factory = EntityIdFactory.getFactory(layout);

    if (entityFlag.startsWith(HBASE_ROW_KEY_SPEC_PREFIX)) {
      // HBase row key specification
      final String hbaseSpec = entityFlag.substring(HBASE_ROW_KEY_SPEC_PREFIX.length());
      final byte[] hbaseRowKey = parseBytesFlag(hbaseSpec);
      return factory.getEntityIdFromHBaseRowKey(hbaseRowKey);

    } else {
      // Kiji row key specification
      final String kijiSpec = entityFlag.startsWith(KIJI_ROW_KEY_SPEC_PREFIX)
          ? entityFlag.substring(KIJI_ROW_KEY_SPEC_PREFIX.length())
          : entityFlag;

      return parseKijiRowKey(kijiSpec, factory, layout);
    }
  }

  /**
   * Parses a Kiji row key specification from a command-line flag.
   *
   * @param rowKeySpec Kiji row key specification.
   * @param factory Factory for entity IDs.
   * @param layout Layout of the table to parse the entity ID of.
   * @return the parsed entity ID.
   * @throws IOException on I/O error.
   */
  public static EntityId parseKijiRowKey(
      String rowKeySpec, EntityIdFactory factory, KijiTableLayout layout)
      throws IOException {

    final Object keysFormat = layout.getDesc().getKeysFormat();
    if (keysFormat instanceof RowKeyFormat) {
      // Former, deprecated, unformatted row key specification:
      return factory.getEntityId(rowKeySpec);

    } else if (keysFormat instanceof RowKeyFormat2) {
      final RowKeyFormat2 format = (RowKeyFormat2) keysFormat;
      switch (format.getEncoding()) {
      case RAW: return factory.getEntityIdFromHBaseRowKey(parseBytesFlag(rowKeySpec));
      case FORMATTED: return parseJsonFormattedKeySpec(rowKeySpec, format, factory);
      default:
        throw new RuntimeException(String.format(
            "Invalid layout for table '%s' with unsupported keys format: '%s'.",
            layout.getName(), format));
      }

    } else {
      throw new RuntimeException(String.format("Unknown row key format: '%s'.", keysFormat));
    }
  }

  /**
   * Converts a JSON string or integer node into a Java object (String, Integer or Long).
   *
   * @param node JSON string or integer numeric node.
   * @return the JSON value, as a String, an Integer or a Long instance.
   * @throws IOException if the JSON node is neither a string nor an integer value.
   */
  private static Object getJsonStringOrIntValue(JsonNode node) throws IOException {
    if (node.isInt() || node.isLong()) {
      return node.getNumberValue();
    } else if (node.isTextual()) {
      return node.getTextValue();
    } else if (node.isNull()) {
      return null;
    } else {
      throw new IOException(String.format(
          "Invalid JSON value: '%s', expecting string, int, long, or null.", node));
    }
  }

  /**
   * Parses a JSON formatted row key specification.
   *
   * @param json JSON specification of the formatted row key.
   *     Either a JSON ordered array, a JSON map (object), or an immediate JSON primitive.
   * @param format Row key format specification from the table layout.
   * @param factory Entity ID factory.
   * @return the parsed entity ID.
   * @throws IOException on I/O error.
   */
  public static EntityId parseJsonFormattedKeySpec(
      String json, RowKeyFormat2 format, EntityIdFactory factory)
      throws IOException {
    try {
      final ObjectMapper mapper = new ObjectMapper();
      final JsonParser parser = new JsonFactory().createJsonParser(json)
          .enable(Feature.ALLOW_COMMENTS)
          .enable(Feature.ALLOW_SINGLE_QUOTES)
          .enable(Feature.ALLOW_UNQUOTED_FIELD_NAMES);
      final JsonNode node = mapper.readTree(parser);
      if (node.isArray()) {
        final Object[] components = new Object[node.size()];
        for (int i = 0; i < node.size(); ++i) {
          components[i] = getJsonStringOrIntValue(node.get(i));
        }
        return factory.getEntityId(components);
      } else if (node.isObject()) {
        // TODO: Implement map row key specifications:
        throw new RuntimeException("Map row key specifications are not implemented yet.");
      } else {
        return factory.getEntityId(getJsonStringOrIntValue(node));
      }

    } catch (JsonParseException jpe) {
      throw new IOException(jpe);
    }
  }

  /** Prefix to specify a sequence of bytes in hexadecimal, as in: "00dead88beefaa". */
  public static final String BYTES_SPEC_PREFIX_HEX = "hex:";

  /** Prefix to specify a sequence of bytes as a URL, as in: "URL%20encoded". */
  public static final String BYTES_SPEC_PREFIX_URL = "url:";

  /** Optional prefix to specify a sequence of bytes in UTF-8, as in: "utf-8 \x00 encoded". */
  public static final String BYTES_SPEC_PREFIX_UTF8 = "utf8:";

  // Support other encoding, eg base64 encoding?

  /**
   * Parses a command-line flag specifying a byte array.
   *
   * Valid specifications are:
   *   <li> UTF-8 encoded strings, as in "utf8:encoded \x00 text".
   *   <li> Hexadecimal sequence, with "hex:00dead88beefaa".
   *   <li> URL encoded strings, as in "url:this%20is%20a%20URL".
   *
   * UTF-8 is the default, hence the "utf8:" prefix is optional unless there is an ambiguity with
   * other prefixes.
   *
   * @param flag Command-line flag specification for a byte array.
   * @return the decoded byte array.
   * @throws IOException on I/O error.
   */
  public static byte[] parseBytesFlag(String flag) throws IOException {
    if (flag.startsWith(BYTES_SPEC_PREFIX_HEX)) {
      // Hexadecimal encoded byte array:
      return ByteArrayFormatter.parseHex(flag.substring(BYTES_SPEC_PREFIX_HEX.length()));

    } else if (flag.startsWith(BYTES_SPEC_PREFIX_URL)) {
      // URL encoded byte array:
      try {
        return URLCodec.decodeUrl(Bytes.toBytes(flag));
      } catch (DecoderException de) {
        throw new IOException(de);
      }

    } else {
      // UTF-8 encoded and escaped byte array:
      final String spec = flag.startsWith(BYTES_SPEC_PREFIX_UTF8)
          ? flag.substring(BYTES_SPEC_PREFIX_UTF8.length())
          : flag;
      return Bytes.toBytes(spec);
    }
  }

  /**
   * Prints cell data from the <code>row</code> for each column specified on the
   * <code>request</code>.
   *
   * @param row The row to read from.
   * @param mapTypeFamilies The map type families to print.
   * @param groupTypeColumns The group type columns to print.
   * @param printStream The stream to print to.
   * @throws IOException if there is an error retrieving data from the KijiRowData.
   */
  public static void printRow(
      KijiRowData row,
      Map<FamilyLayout, List<String>> mapTypeFamilies,
      Map<FamilyLayout, List<ColumnLayout>> groupTypeColumns,
      PrintStream printStream)
      throws IOException {

    // Unpack and print result for the map type families.
    for (Entry<FamilyLayout, List<String>> entry : mapTypeFamilies.entrySet()) {
      final FamilyLayout family = entry.getKey();
      if (family.getDesc().getMapSchema().getType() == SchemaType.COUNTER) {

        // If this map family of counters has no qualifiers, print entire family.
        if (entry.getValue().isEmpty()) {
          for (String key : row.getQualifiers(family.getName())) {
            KijiCell<Long> counter = row.getMostRecentCell(family.getName(), key);
            if (null != counter) {
              printCell(row.getEntityId(), counter, printStream);
            }
          }
        // If this map family of counters has been qualified, print only the given columns.
        } else {
          for (String key : entry.getValue()) {
            KijiCell<Long> counter = row.getMostRecentCell(family.getName(), key);
            if (null != counter) {
              printCell(row.getEntityId(), counter, printStream);
            }
          }
        }
      } else {
        // If this map family of non-counters has no qualifiers, print entire family.
        if (entry.getValue().isEmpty()) {
          NavigableMap<String, NavigableMap<Long, Object>> keyTimeseriesMap =
              row.getValues(family.getName());
          for (String key : keyTimeseriesMap.keySet()) {
            for (Entry<Long, Object> timestampedCell : keyTimeseriesMap.get(key).entrySet()) {
              long timestamp = timestampedCell.getKey();
              printCell(row.getEntityId(), timestamp, family.getName(), key,
                  timestampedCell.getValue(), printStream);
            }
          }
        // If this map family of non-counters has been qualified, print only the given columns.
        } else {
          for (String key : entry.getValue()) {
            NavigableMap<Long, Object> timeseriesMap =
                row.getValues(family.getName(), key);
            for (Entry<Long, Object> timestampedCell : timeseriesMap.entrySet()) {
              long timestamp = timestampedCell.getKey();
              printCell(
                  row.getEntityId(), timestamp, family.getName(), key, timestampedCell.getValue(),
                  printStream);
            }
          }
        }
      }
    }

    // Unpack and print result for the group type families.
    for (Entry<FamilyLayout, List<ColumnLayout>> entry : groupTypeColumns.entrySet()) {
      String familyName = entry.getKey().getName();
      for (ColumnLayout column : entry.getValue()) {
        final KijiColumnName colName = KijiColumnName.create(familyName, column.getName());
        if (column.getDesc().getColumnSchema().getType() == SchemaType.COUNTER) {
          final KijiCell<Long> counter =
              row.getMostRecentCell(colName.getFamily(), colName.getQualifier());
          if (null != counter) {
            printCell(row.getEntityId(), counter, printStream);
          }
        } else {
          for (Entry<Long, Object> timestampedCell
              : row.getValues(colName.getFamily(), colName.getQualifier())
                  .entrySet()) {
            long timestamp = timestampedCell.getKey();
            printCell(row.getEntityId(), timestamp, colName.getFamily(),
                colName.getQualifier(), timestampedCell.getValue(), printStream);
          }
        }
      }
    }
    printStream.println("");
  }

  /**
   * Prints the contents of a single kiji cell to the printstream.
   *
   * @param entityId The entity id.
   * @param timestamp This timestamp of a KijiCell.
   * @param family The family of a KijiCell.
   * @param qualifier The qualifier of a KijiCell.
   * @param cellData The contents of a KijiCell.
   * @param printStream The stream to print to.
   */
  private static void printCell(EntityId entityId, Long timestamp,
      String family, String qualifier, Object cellData, PrintStream printStream) {
    printStream.printf("entity-id=%s [%d] %s:%s%n                                 %s%n",
        formatEntityId(entityId),
        timestamp,
        family,
        qualifier,
        cellData);
  }

  /**
   * Prints the contents of a single kiji cell to the printstream.
   *
   * @param entityId The entity id.
   * @param cell The KijiCell.
   * @param printStream The stream to print to.
   */
  private static void printCell(EntityId entityId, KijiCell<?> cell, PrintStream printStream) {
    printStream.printf("entity-id=%s [%d] %s:%s%n                                 %s%n",
        formatEntityId(entityId),
        cell.getTimestamp(),
        cell.getColumn().getFamily(),
        cell.getColumn().getQualifier(),
        cell.getData());
  }

  /**
   * Returns the list of map-type families specified by <code>rawColumns</code>.
   * If <code>rawColumns</code> is null, then all map-type families are returned.
   *
   * @param rawColumnNames The raw columns supplied by the user.
   * @param layout The KijiTableLayout.
   * @return A list of map type families specified by the raw columns.
   */
  public static Map<FamilyLayout, List<String>> getMapTypeFamilies(
      List<KijiColumnName> rawColumnNames,
      KijiTableLayout layout) {
    final Map<FamilyLayout, List<String>> familyMap = Maps.newHashMap();
    if (rawColumnNames.isEmpty()) {
      for (FamilyLayout family : layout.getFamilies()) {
        if (family.isMapType()) {
          familyMap.put(family, new ArrayList<String>());
        }
      }
    } else {
      for (KijiColumnName rawColumn : rawColumnNames) {
        final FamilyLayout family = layout.getFamilyMap().get(rawColumn.getFamily());
        if (null == family) {
          throw new RuntimeException(String.format(
              "No family '%s' in table '%s'.", rawColumn.getFamily(), layout.getName()));
        }
        if (family.isMapType()) {
          addColumn(family, rawColumn.getQualifier(), familyMap);
        }
      }
    }
    return familyMap;
  }

  /**
   * Returns the list of group-type columns specified by <code>rawColumns</code>.
   * If <code>rawColumns</code> is null, then all columns in all group-type families are returned.
   * If a raw column specifies a group-type family, but no qualifier, then each column in that
   * family is returned.
   *
   * @param rawColumnNames The raw columns supplied by the user.
   * @param layout The KijiTableLayout.
   * @return The fully qualified columns specified by the raw columns.
   */
  public static Map<FamilyLayout, List<ColumnLayout>> getGroupTypeColumns(
      List<KijiColumnName> rawColumnNames,
      KijiTableLayout layout) {
    final Map<FamilyLayout, List<ColumnLayout>> familyMap = Maps.newHashMap();
    if (rawColumnNames.isEmpty()) {
      for (FamilyLayout family : layout.getFamilies()) {
        if (family.isGroupType()) {
          familyMap.put(family, Lists.newArrayList(family.getColumns()));
        }
      }
    } else {
      for (KijiColumnName rawColumn : rawColumnNames) {
        final FamilyLayout family = layout.getFamilyMap().get(rawColumn.getFamily());
        if (null == family) {
          throw new RuntimeException(String.format(
              "No family '%s' in table '%s'.", rawColumn.getFamily(), layout.getName()));
        }
        if (family.isGroupType()) {
          // We'll include it.  Is it fully qualified?
          if (!rawColumn.isFullyQualified()) {
            // User specified a group-type family, but no qualifier.  Include all qualifiers.
            for (ColumnLayout column : family.getColumns()) {
              addColumn(family, column, familyMap);
            }
          } else {
            final ColumnLayout column = family.getColumnMap().get(rawColumn.getQualifier());
            if (null == column) {
              throw new RuntimeException(String.format(
                  "No column '%s' in table '%s'.", rawColumn, layout.getName()));
            }
            addColumn(family, column, familyMap);
          }
        }
      }
    }
    return familyMap;
  }

  /**
   * Adds a column to the list of columns mapped from <code>family</code>.
   *
   * @param family The family as a key.
   * @param column The column to add to the list of columns for this family.
   * @param familyColumnMap The map between families and lists of columns.
   */
  private static void addColumn(
      FamilyLayout family,
      ColumnLayout column,
      Map<FamilyLayout, List<ColumnLayout>> familyColumnMap) {
    if (!familyColumnMap.containsKey(family)) {
      familyColumnMap.put(family, new ArrayList<ColumnLayout>());
    }
    familyColumnMap.get(family).add(column);
  }

  /**
   * Adds a column to the list of columns mapped from <code>family</code>.
   *
   * @param mapFamily The map family as a key.
   * @param qualifier The qualifier to add to the list of qualifiers for this map family.
   * @param familyQualifierMap The map between map families and lists of qualifiers.
   */
  private static void addColumn(
      FamilyLayout mapFamily,
      String qualifier,
      Map<FamilyLayout, List<String>> familyQualifierMap) {
    if (!familyQualifierMap.containsKey(mapFamily)) {
      familyQualifierMap.put(mapFamily, new ArrayList<String>());
    }
    if (null != qualifier) {
      familyQualifierMap.get(mapFamily).add(qualifier);
    }
  }

  /**
   * Returns a KijiDataRequest for the specified columns.  If columns is null,
   * returns a request for all columns.
   *
   * @param mapTypeFamilies The list of map type families to include.
   * @param groupTypeColumns The family:qualifier map of group type columns to include.
   * @param maxVersions The max versions to include.
   * @param minTimestamp The min timestamp.
   * @param maxTimestamp The max timestamp.
   * @return The KijiDataRequest.
   */
  public static KijiDataRequest getDataRequest(
      Map<FamilyLayout, List<String>> mapTypeFamilies,
      Map<FamilyLayout, List<ColumnLayout>> groupTypeColumns,
      int maxVersions,
      long minTimestamp,
      long maxTimestamp) {
    final KijiDataRequestBuilder builder = KijiDataRequest.builder()
        .withTimeRange(minTimestamp, maxTimestamp);

    final KijiDataRequestBuilder.ColumnsDef colBuilder =
        builder.newColumnsDef().withMaxVersions(maxVersions);

    for (Entry<FamilyLayout, List<String>> entry : mapTypeFamilies.entrySet()) {
      String familyName = entry.getKey().getName();
      // If the map family is without qualifiers, add entire family.
      if (entry.getValue().isEmpty()) {
        LOG.debug("Adding family to data request: " + familyName);
        colBuilder.addFamily(familyName);
      } else {
        // If the map family is with qualifiers, add only the columns of interest.
        for (String qualifier : entry.getValue()) {
          LOG.debug("Adding column to data request: " + familyName + ":" + qualifier);
          colBuilder.add(familyName, qualifier);
        }
      }
    }

    for (Entry<FamilyLayout, List<ColumnLayout>> entry : groupTypeColumns.entrySet()) {
      String familyName = entry.getKey().getName();
      for (ColumnLayout column : entry.getValue()) {
        LOG.debug("Adding column to data request: " + column.getName());
        colBuilder.add(familyName, column.getName());
      }
    }
    return builder.build();
  }

  /**
   * Formats an entity ID for a command-line user.
   *
   * @deprecated use {@link EntityId#toShellString()} instead.
   * @param eid Entity ID to format.
   * @return the formatted entity ID as a String to print on the console.
   */
  @Deprecated
  public static String formatEntityId(EntityId eid) {
    return EntityIdFactory.formatEntityId(eid);
  }
}
