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
package org.kiji.schema.tools;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.common.flags.Flag;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableAnnotator;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;

/**
 * CLI for setting, removing, and getting column and table annotations.
 *
 * <h3>Example Usage:</h3>
 * <p>
 *   <ul>
 *     <li>
 *       Set sample_key -> sample_value for table: "table":
 *       <pre>
 *         kiji notes --target=kiji://.env/default/table \
 *             --do=set \
 *             --key=sample_key \
 *             --value=sample_value
 *       </pre>
 *     </li>
 *     <li>
 *       Set sample_key -> sample_value and sample_key_2 -> sample_value_2 for qualified column
 *       "family:qualfiier" in table "table":
 *       <pre>
 *         kiji notes --target=kiji://.env/default/table/family:qualifier \
 *             --do=set \
 *             --map={"sample_key":"sample_value","sample_key_2":"sample_value_2"}
 *       </pre>
 *     </li>
 *     <li>
 *       Remove all annotations from "family:qualifier" in table "table":
 *       <pre>
 *         kiji notes --target=kiji://.env/default/table/family:qualifier \
 *             --do=remove
 *       </pre>
 *     </li>
 *     <li>
 *       Get all annotations starting with the given key prefix on columns in the family "family" in
 *       table "table":
 *       <pre>
 *         kiji notes --target=kiji://.env/default/table \
 *             --do=get
 *             --in-family=family
 *             --key=sample_
 *             --prefix
 *       </pre>
 *     </li>
 *     <li>
 *       Get all annotations matching the given regex (letters only) on the family "family" (not
 *       columns in "family") in table "table":
 *       <pre>
 *         kiji notes --target=kiji://.env/default/table/family \
 *             --do=get
 *             --key=^[a-zA-Z]*&
 *             --regex
 *       </pre>
 *     </li>
 *   </ul>
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class NotesTool extends BaseTool {

  @Flag(name="target", usage="Specify the KijiURI of the target of your operation. Must include at "
      + "least a table and may include exactly one column.")
  private String mTargetFlag = "";

  @Flag(name="do", usage="Pick exactly one of:\n"
      + "  'set' to set one or more key value pair for a column or table.\n"
      + "    (requires --key and --value OR --map)\n"
      + "  'remove' to remove key value pairs from a column or table.\n"
      + "    (use --prefix, --partial, or --regex to remove keys which match the given criteria.\n"
      + "  'get' to get key value pairs from a column or table.\n"
      + "    (use --prefix, --partial, or --regex to get keys which match the given criteria.\n")
  private String mDoFlag = "";

  @Flag(name="key", usage="Specify an annotation key. Keys must match the pattern "
      + KijiTableAnnotator.ALLOWED_ANNOTATION_KEY_PATTERN + "\n  (use --prefix, --partial, or "
      + "--regex to treat this as a matcher instead of an exact key.)\n  (if unset while "
      + "--do=remove or --do=get, all annotations will be removed or retrieved)")
  private String mKeyFlag = "";

  @Flag(name="value", usage="Specify an annotation value. Values may be any string.")
  private String mValueFlag = "";

  @Flag(name="map", usage="Specify a json map of annotation key-values. Keys must match the "
      + "pattern " + KijiTableAnnotator.ALLOWED_ANNOTATION_KEY_PATTERN + ", values may be any "
      + "string.")
  private String mMapFlag = "";

  @Flag(name="prefix", usage="Specify to treat the --key flag as a key prefix.\n"
      + "Usable only with --do=get")
  private Boolean mPrefixFlag = false;

  @Flag(name="partial", usage="Specify to treat the --key flag as a partial key.\n"
      + "Usable only with --do=get")
  private Boolean mPartialFlag = false;

  @Flag(name="regex", usage="Specify to treat the --key flag as a regex pattern.\n"
      + "Usable only with --do=get")
  private Boolean mRegexFlag = false;

  @Flag(name="in-family", usage="Specify to get or remove keys from the given family.\n"
      + "  (requires no columns specified in --target)")
  private String mFamilyFlag = "";

  @Flag(name="all-columns", usage="Specify to get or remove keys from all columns in the target "
      + "table, rather than the table itself.\n"
      + "  (requires no columns specified in --target and --do=remove or --do=get)")
  private Boolean mAllColumnsFlag = false;

  private DoMode mDoMode = null;
  private KijiURI mURI = null;
  private KijiTableAnnotator mAnnotator = null;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "notes";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Set, remove, and get column and table annotations.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Metadata";
  }

  /** Possible operations of this tool. */
  private static enum DoMode {
    SET, REMOVE, GET
  }

  private static final Gson GSON = new Gson();

  /**
   * Deserialize a String-String map from JSON.
   *
   * @param json the map to deserialize.
   * @return the String-String map encoded in the json.
   */
  private static Map<String, String> mapFromJson(
      final String json
  ) {
    return GSON.fromJson(json, Map.class);
  }

  /**
   * Check if more than one of the specified booleans is true.
   *
   * @param bools the booleans to check.
   * @return whether mroe than one of the specified booleans is true.
   */
  private static boolean moreThanOne(boolean... bools) {
    int totalTrue = 0;
    for (boolean b : bools) {
      if (b) {
        totalTrue++;
      }
    }
    return totalTrue > 1;
  }

  /**
   * Print an annotation on the table specified in mURI.
   *
   * @param key the annotation key.
   * @param value the annotation value.
   */
  private void printTableAnnotation(
      final String key,
      final String value
  ) {
    getPrintStream().printf("table: '%s'%n"
        + "  '%s': '%s'%n", mURI.getTable(), key, value);
  }

  /**
   * Print several annotation key values on the table specified in mURI.
   *
   * @param kvs a mapping of keys and values.
   */
  private void printTableAnnotations(
      final Map<String, String> kvs
  ) {
    getPrintStream().printf("table: '%s'%n", mURI.getTable());
    for (Map.Entry<String, String> kv : kvs.entrySet()) {
      getPrintStream().printf("  '%s': '%s'%n", kv.getKey(), kv.getValue());
    }
  }

  /**
   * Print a column and an annotation key value.
   *
   * @param column the annotated column.
   * @param key the annotation key.
   * @param value the annotation value.
   */
  private void printColumnAnnotation(
      final KijiColumnName column,
      final String key,
      final String value
  ) {
    getPrintStream().printf("column: '%s'%n"
        + "  '%s': '%s'%n", column, key, value);
  }

  /**
   * Print a column and several annotation key values.
   *
   * @param column the annotated column.
   * @param kvs a mapping of the keys and values.
   */
  private void printColumnAnnotations(
      final KijiColumnName column,
      final Map<String, String> kvs
  ) {
    getPrintStream().printf("column: '%s'%n", column);
    for (Map.Entry<String, String> kv: kvs.entrySet()) {
      getPrintStream().printf("  '%s': '%s'%n", kv.getKey(), kv.getValue());
    }
  }

  /**
   * Print several column annotations with the same key.
   *
   * @param key the key for all annotations.
   * @param cvs a map from columns to the value at the given annotation key.
   */
  private void printColumnAnnotations(
      final String key,
      final Map<KijiColumnName, String> cvs
  ) {
    for (Map.Entry<KijiColumnName, String> cvEntry : cvs.entrySet()) {
      printColumnAnnotation(cvEntry.getKey(), key, cvEntry.getValue());
    }
  }

  /**
   * Print several columns and annotations associated with those columns.
   *
   * @param ckvs a mapping from column to key to value.
   */
  private void printColumnAnnotations(
      final Map<KijiColumnName, Map<String, String>> ckvs
  ) {
    for (Map.Entry<KijiColumnName, Map<String, String>> columnEntry : ckvs.entrySet()) {
      printColumnAnnotations(columnEntry.getKey(), columnEntry.getValue());
    }
  }

  /**
   * Print a removed table annotation.
   *
   * @param removedKey the removed annotation key.
   */
  private void printRemovedTableAnnotation(
      final String removedKey
  ) {
    getPrintStream().printf("table: '%s'%n  '%s'%n", mURI.getTable(), removedKey);
  }

  /**
   * Print several removed table annotations.
   *
   * @param removedKeys the set of removed annotation keys.
   */
  private void printRemovedTableAnnotations(
      final Set<String> removedKeys
  ) {
    getPrintStream().printf("table: '%s'%n", mURI.getTable());
    for (String key : removedKeys) {
      getPrintStream().printf("  '%s'%n", key);
    }
  }

  /**
   * Print a removed column annotation.
   *
   * @param column column from which the annotation was removed.
   * @param removedKey removed annotation key.
   */
  private void printRemovedColumnAnnotation(
      final KijiColumnName column,
      final String removedKey
  ) {
    getPrintStream().printf("column: '%s'%n  '%s'", column, removedKey);
  }

  /**
   * Print several removed annotations from a single column.
   *
   * @param column column from which annotations were removed.
   * @param removedKeys removed annotation keys.
   */
  private void printRemovedColumnAnnotations(
      final KijiColumnName column,
      final Set<String> removedKeys
  ) {
    getPrintStream().printf("column: '%s'%n", column);
    for (String key: removedKeys) {
      getPrintStream().printf("  '%s'%n", key);
    }
  }

  /**
   * Print several removed annotations from several columns.
   *
   * @param removedKeys map from column to annotation keys removed from that column.
   */
  private void printRemovedColumnAnnotations(
      final Map<KijiColumnName, Set<String>> removedKeys
  ) {
    for (Map.Entry<KijiColumnName, Set<String>> columnEntry : removedKeys.entrySet()) {
      printRemovedColumnAnnotations(columnEntry.getKey(), columnEntry.getValue());
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() {
    try {
      mURI = KijiURI.newBuilder(mTargetFlag).build();
    } catch (KijiURIException kurie) {
      exitWithErrorMessage(kurie.getMessage());
    }
    if (null == mURI.getTable()) {
      exitWithFormattedErrorMessage("--target URI must specify at least a table, got: '%s'", mURI);
    }
    if (1 < mURI.getColumns().size()) {
      exitWithErrorMessage("--target URI must include one or zero columns.");
    }
    try {
      mDoMode = DoMode.valueOf(mDoFlag.toUpperCase(Locale.ROOT).replace('-', '_'));
    } catch (IllegalArgumentException iae) {
      exitWithFormattedErrorMessage(
          "Invalid --do command: '%s'. Valid commands are set, remove, get", mDoFlag);
    }
    switch (mDoMode) {
      case SET: {
        if (mPrefixFlag || mPartialFlag || mRegexFlag) {
          exitWithErrorMessage("Cannot specify --prefix, --partial, or --regex while --do=set");
        }
        if (!mFamilyFlag.isEmpty()) {
          exitWithErrorMessage("Cannot specify a family with --in-family while --do=set");
        }
        if (mMapFlag.isEmpty() && (mKeyFlag.isEmpty() || mValueFlag.isEmpty())) {
          exitWithErrorMessage("--do=set requires --key and --value OR --map");
        }
        if (!mMapFlag.isEmpty() && (!mKeyFlag.isEmpty() || !mValueFlag.isEmpty())) {
          exitWithErrorMessage("--map is mutually exclusive with --key and --value");
        }
        break;
      }
      case REMOVE: {
        if (moreThanOne(mPrefixFlag, mPartialFlag, mRegexFlag)) {
          exitWithErrorMessage("Cannot specify more than one of --prefix, --partial, and --regex");
        }
        if (!mMapFlag.isEmpty()) {
          exitWithErrorMessage("Cannot specify --map while --do=remove");
        }
        if (!mValueFlag.isEmpty()) {
          exitWithErrorMessage("Cannot specify --value while --do=remove");
        }
        break;
      }
      case GET: {
        if (moreThanOne(mPrefixFlag, mPartialFlag, mRegexFlag)) {
          exitWithErrorMessage("Cannot specify more than one of --prefix, --partial, and --regex");
        }
        if (!mMapFlag.isEmpty()) {
          exitWithErrorMessage("Cannot specify --map while --do=get");
        }
        if (!mValueFlag.isEmpty()) {
          exitWithErrorMessage("Cannot specify --value while --do=get");
        }
        break;
      }
      default: throw new InternalKijiError(String.format("Unknown DoMode: %s", mDoMode));
    }
    if (!mFamilyFlag.isEmpty() && !mURI.getColumns().isEmpty()) {
      exitWithErrorMessage("--in-family requires that no columns are specified in --target");
    }
  }

  /**
   * Set annotations.
   *
   * @return a tool exit code.
   * @throws IOException in case of an error writing to the meta table.
   */
  private int set() throws IOException {
    final List<KijiColumnName> columns = mURI.getColumns();
    if (columns.isEmpty()) {
      if (mMapFlag.isEmpty()) {
        mAnnotator.setTableAnnotation(mKeyFlag, mValueFlag);
        getPrintStream().println("Annotation added:");
        printTableAnnotation(mKeyFlag, mValueFlag);
      } else {
        final Map<String, String> kvs = mapFromJson(mMapFlag);
        mAnnotator.setTableAnnotations(kvs);
        getPrintStream().println("Annotations added:");
        printTableAnnotations(kvs);
      }
    } else {
      for (KijiColumnName column : columns) {
        if (mMapFlag.isEmpty()) {
          mAnnotator.setColumnAnnotation(column, mKeyFlag, mValueFlag);
          getPrintStream().println("Annotation added:");
          printColumnAnnotation(column, mKeyFlag, mValueFlag);
        } else {
          final Map<String, String> kvs = mapFromJson(mMapFlag);
          mAnnotator.setColumnAnnotations(column, kvs);
          getPrintStream().println("Annotations added:");
          printColumnAnnotations(column, kvs);
        }
      }
    }
    return BaseTool.SUCCESS;
  }

  /**
   * Remove annotations.
   *
   * @return a tool exit code.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  private int remove() throws IOException {
    final List<KijiColumnName> columns = mURI.getColumns();
    if (columns.isEmpty()) {
      if (mKeyFlag.isEmpty()) {
        if (mAllColumnsFlag) {
          getPrintStream().println("Annotations removed:");
          printRemovedColumnAnnotations(mAnnotator.removeAllColumnAnnotations());
        } else {
          if (!mFamilyFlag.isEmpty()) {
            getPrintStream().println("Annotations removed:");
            printRemovedColumnAnnotations(
                mAnnotator.removeAllColumnAnnotationsInFamily(mFamilyFlag));
          } else {
            getPrintStream().println("Annotations removed:");
            printRemovedTableAnnotations(mAnnotator.removeAllTableAnnotations());
          }
        }
      } else {
        if (mFamilyFlag.isEmpty()) {
          if (mPrefixFlag) {
            getPrintStream().println("Annotations removed:");
            printRemovedTableAnnotations(mAnnotator.removeTableAnnotationsStartingWith(mKeyFlag));
          } else if (mPartialFlag) {
            getPrintStream().println("Annotations removed:");
            printRemovedTableAnnotations(mAnnotator.removeTableAnnotationsContaining(mKeyFlag));
          } else if (mRegexFlag) {
            getPrintStream().println("Annotations removed:");
            printRemovedTableAnnotations(mAnnotator.removeTableAnnotationsMatching(mKeyFlag));
          } else {
            getPrintStream().println("Annotation removed:");
            mAnnotator.removeTableAnnotation(mKeyFlag);
            printRemovedTableAnnotation(mKeyFlag);
          }
        } else {
          if (mPrefixFlag) {
            getPrintStream().println("Annotations removed:");
            printRemovedColumnAnnotations(
                mAnnotator.removeColumnAnnotationsInFamilyStartingWith(mFamilyFlag, mKeyFlag));
          } else if (mPartialFlag) {
            getPrintStream().println("Annotations removed:");
            printRemovedColumnAnnotations(
                mAnnotator.removeColumnAnnotationsInFamilyContaining(mFamilyFlag, mKeyFlag));
          } else if (mRegexFlag) {
            getPrintStream().println("Annotations removed:");
            printRemovedColumnAnnotations(
                mAnnotator.removeColumnAnnotationsInFamilyMatching(mFamilyFlag, mKeyFlag));
          } else {
            final Set<KijiColumnName> removedColumns =
                mAnnotator.removeColumnAnnotationsInFamily(mFamilyFlag, mKeyFlag);
            getPrintStream().println("Annotations removed:");
            getPrintStream().printf("key: '%s'%n", mKeyFlag);
            for (KijiColumnName column : removedColumns) {
              getPrintStream().printf("  '%s'%n", column);
            }
          }
        }
      }
    } else {
      if (mKeyFlag.isEmpty()) {
        for (KijiColumnName column : columns) {
          getPrintStream().println("Annotations removed:");
          printRemovedColumnAnnotations(column, mAnnotator.removeAllColumnAnnotations(column));
        }
      } else {
        for (KijiColumnName column : columns) {
          if (mPrefixFlag) {
            getPrintStream().println("Annotations removed:");
            printRemovedColumnAnnotations(
                column, mAnnotator.removeColumnAnnotationsStartingWith(column, mKeyFlag));
          } else if (mPartialFlag) {
            getPrintStream().println("Annotations removed:");
            printRemovedColumnAnnotations(
                column, mAnnotator.removeColumnAnnotationsContaining(column, mKeyFlag));
          } else if (mRegexFlag) {
            getPrintStream().println("Annotations removed:");
            printRemovedColumnAnnotations(
                column, mAnnotator.removeColumnAnnotationsMatching(column, mKeyFlag));
          } else {
            getPrintStream().println("Annotation removed:");
            mAnnotator.removeColumnAnnotation(column, mKeyFlag);
            printRemovedColumnAnnotation(column, mKeyFlag);
          }
        }
      }
    }
    return BaseTool.SUCCESS;
  }

  /**
   * Get and print annotations.
   *
   * @return a tool exit code.
   * @throws IOException in case of an error reading from the meta table.
   */
  private int get() throws IOException {
    final List<KijiColumnName> columns = mURI.getColumns();
    if (columns.isEmpty()) {
      if (mKeyFlag.isEmpty()) {
        if (mAllColumnsFlag) {
          printColumnAnnotations(mAnnotator.getAllColumnAnnotations());
        } else {
          if (!mFamilyFlag.isEmpty()) {
            printColumnAnnotations(mAnnotator.getAllColumnAnnotationsInFamily(mFamilyFlag));
          } else {
            printTableAnnotations(mAnnotator.getAllTableAnnotations());
          }
        }
      } else {
        if (mFamilyFlag.isEmpty()) {
          if (mPrefixFlag) {
            printTableAnnotations(mAnnotator.getTableAnnotationsStartingWith(mKeyFlag));
          } else if (mPartialFlag) {
            printTableAnnotations(mAnnotator.getTableAnnotationsContaining(mKeyFlag));
          } else if (mRegexFlag) {
            printTableAnnotations(mAnnotator.getTableAnnotationsMatching(mKeyFlag));
          } else  {
            printTableAnnotation(mKeyFlag, mAnnotator.getTableAnnotation(mKeyFlag));
          }
        } else {
          if (mPrefixFlag) {
            printColumnAnnotations(
                mAnnotator.getColumnAnnotationsInFamilyStartingWith(mFamilyFlag, mKeyFlag));
          } else if (mPartialFlag) {
            printColumnAnnotations(
                mAnnotator.getColumnAnnotationsInFamilyContaining(mFamilyFlag, mKeyFlag));
          } else if (mRegexFlag) {
            printColumnAnnotations(
                mAnnotator.getColumnAnnotationsInFamilyMatching(mFamilyFlag, mKeyFlag));
          } else  {
            printColumnAnnotations(
                mKeyFlag, mAnnotator.getColumnAnnotationsInFamily(mFamilyFlag, mKeyFlag));
          }
        }
      }
    } else {
      if (mKeyFlag.isEmpty()) {
        for (KijiColumnName column : columns) {
          printColumnAnnotations(column, mAnnotator.getAllColumnAnnotations(column));
        }
      } else {
        for (KijiColumnName column : columns) {
          if (mPrefixFlag) {
            printColumnAnnotations(
                column, mAnnotator.getColumnAnnotationsStartingWith(column, mKeyFlag));
          } else if (mPartialFlag) {
            printColumnAnnotations(
                column, mAnnotator.getColumnAnnotationsContaining(column, mKeyFlag));
          } else if (mRegexFlag) {
            printColumnAnnotations(
                column, mAnnotator.getColumnAnnotationsMatching(column, mKeyFlag));
          } else {
            printColumnAnnotation(
                column, mKeyFlag, mAnnotator.getColumnAnnotation(column, mKeyFlag));
          }
        }
      }
    }
    return BaseTool.SUCCESS;
  }

  /** {@inheritDoc} */
  @Override
  protected int run(final List<String> nonFlagArgs) throws Exception {
    final Kiji kiji = Kiji.Factory.open(mURI);
    try {
      final KijiTable table = kiji.openTable(mURI.getTable());
      try {
        mAnnotator = table.openTableAnnotator();
        try {
          switch (mDoMode) {
            case SET: return set();
            case REMOVE: return remove();
            case GET: return get();
            default: throw new InternalKijiError("Unknown NotesTool DoMode: " + mDoMode);
          }
        } finally {
          mAnnotator.close();
        }
      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }
  }
}
