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
package org.kiji.schema.impl.hbase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableAnnotator;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.HBaseColumnNameTranslator;

/** HBase implementation of {@link org.kiji.schema.KijiTableAnnotator}. */
@ApiAudience.Private
public final class HBaseKijiTableAnnotator implements KijiTableAnnotator {

  static final String METATABLE_KEY_PREFIX = "kiji.schema.table_annotator.";

  // -----------------------------------------------------------------------------------------------
  // Static methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Check if the given annotation key is valid.
   *
   * @param key the annotation key to check.
   * @return whether the key is valid.
   */
  public static boolean isValidAnnotationKey(
      final String key
  ) {
    return key.matches(ALLOWED_ANNOTATION_KEY_PATTERN);
  }

  /**
   * Validate that a given key is a valid KijiTableAnnotator annotation key.
   *
   * @param key the annotation key to check for validity.
   * @throws IllegalArgumentException if the key is invalid.
   */
  private static void validateAnnotationKey(
      final String key
  ) {
    Preconditions.checkArgument(isValidAnnotationKey(key),
        "Annotation keys much match pattern: %s, found: %s", ALLOWED_ANNOTATION_KEY_PATTERN, key);
  }

  /**
   * Check if a metaTableKey is a KijiTableAnnotator meta table key for a column annotation.
   *
   * @param metaTableKey the meta table key to check.
   * @return whether the given meta table key is a KijiTableAnnotator meta table key for a column
   *     annotation.
   */
  private static boolean isKCAColumnMetaTableKey(
      final String metaTableKey
  ) {
    final int firstColonIndex = metaTableKey.indexOf(':');
    final int lastPeriodIndex = metaTableKey.lastIndexOf('.');
    return metaTableKey.startsWith(METATABLE_KEY_PREFIX)
        // a colon between the prefix and the last period indicates a qualified column.
        && ((firstColonIndex > METATABLE_KEY_PREFIX.length() && lastPeriodIndex > firstColonIndex)
        // nothing between the prefix and the last period indicates a table key.
        || firstColonIndex == -1 && lastPeriodIndex != (METATABLE_KEY_PREFIX.length()))
        // +1 to exclude the '.'.
        && isValidAnnotationKey(keyFromMetaTableKey(metaTableKey));
  }

  /**
   * Check if a metaTableKey is a KijiTableAnnotator meta table key for a table annotation.
   *
   * @param metaTableKey the meta table key to check.
   * @return whether the given meta table key is a KijiTableAnnotator meta table key for a table
   *     annotation.
   */
  private static boolean isKCATableMetaTableKey(
      final String metaTableKey
  ) {
    final int lastPeriodIndex = metaTableKey.lastIndexOf('.');
    return metaTableKey.startsWith(METATABLE_KEY_PREFIX)
        // The last period should be just after the prefix.
        && lastPeriodIndex == (METATABLE_KEY_PREFIX.length())
        && isValidAnnotationKey(keyFromMetaTableKey(metaTableKey));
  }

  /**
   * Get the meta table key for the table served by this column annotator and the given annotation
   * key. This method is package private for testing.
   *
   * @param key annotation key for which to get the meta table key.
   * @return the meta table key for the given annotation key.
   */
  static String getMetaTableKey(
      final String key
  ) {
    Preconditions.checkArgument(isValidAnnotationKey(key), "Annotation key: %s does not conform to "
        + "required pattern: %s", key, ALLOWED_ANNOTATION_KEY_PATTERN);
    return String.format("%s.%s", METATABLE_KEY_PREFIX, key);
  }

  /**
   * Get the meta table key for the given column name and annotation key. This method is package
   * private for testing.
   *
   * @param table HBaseKijiTable in which the specified column lives. This will be used for
   *     column name translation.
   * @param columnName the name of the column from which to get the meta table key.
   * @param key the annotation key from which to get the meta table key.
   * @return the meta table key for the given column name and annotation key.
   * @throws NoSuchColumnException in case the column does not exist in the table.
   */
  static String getMetaTableKey(
      final HBaseKijiTable table,
      final KijiColumnName columnName,
      final String key
  ) throws NoSuchColumnException {
    final HBaseColumnNameTranslator translator = table.getColumnNameTranslator();
    Preconditions.checkArgument(isValidAnnotationKey(key), "Annotation key: %s does not conform to "
        + "required pattern: %s", key, ALLOWED_ANNOTATION_KEY_PATTERN);
    return String.format("%s%s.%s",
        METATABLE_KEY_PREFIX, translator.toHBaseColumnName(columnName), key);
  }

  /**
   * Get the annotation key from a meta table key. Does not check if the metaTableKey is a valid
   * KijiTableAnnotator meta table key. This method is package private for testing.
   *
   * @param metaTableKey the meta table key from which to get the annotation key.
   * @return the annotation key stored in the given meta table key.
   */
  static String keyFromMetaTableKey(
      final String metaTableKey
  ) {
    // +1 to exclude the '.'.
    return metaTableKey.substring(metaTableKey.lastIndexOf('.') + 1);
  }

  /**
   * Get the column name from a meta table key. Does not check if the metaTableKey is a valid
   * KijiTableAnnotator meta table key. This method is package private for testing.
   *
   * @param table HBaseKijiTable in which the column from the specified key lives. This will be used
   *     for column name translation.
   * @param metaTableKey the meta table key from which to get the column name.
   * @return the column name stored in the given meta table key.
   * @throws NoSuchColumnException in case the column does not exist in the table.
   */
  static KijiColumnName columnFromMetaTableKey(
      final HBaseKijiTable table,
      final String metaTableKey
  ) throws NoSuchColumnException {
    final HBaseColumnNameTranslator translator = table.getColumnNameTranslator();
    // Everything between the prefix and the annotation key.
    final String hbaseColumnString =
        metaTableKey.substring(METATABLE_KEY_PREFIX.length(), metaTableKey.lastIndexOf('.'));
    // Everything before the first ':'.
    final String hbaseFamily = hbaseColumnString.substring(0, hbaseColumnString.indexOf(':'));
    // Everything after the first ':'. The +1 excludes the ':' itself.
    final String hbaseQualifier = hbaseColumnString.substring(hbaseColumnString.indexOf(':') + 1);

    final HBaseColumnName hbaseColumn = new HBaseColumnName(hbaseFamily, hbaseQualifier);
    return translator.toKijiColumnName(hbaseColumn);
  }

  // -----------------------------------------------------------------------------------------------
  // State
  // -----------------------------------------------------------------------------------------------

  /** Possible states of an annotator. */
  private static enum State {
    OPEN, CLOSED
  }

  private final HBaseKijiTable mTable;
  private final AtomicReference<State> mState;

  /**
   * Initialize a new HBaseKijiTableAnnotator for the given table.
   *
   * @param table the table served by this ColumnAnnotator.
   */
  public HBaseKijiTableAnnotator(
      final HBaseKijiTable table
  ) {
    mTable = (HBaseKijiTable) table.retain();
    mState = new AtomicReference<State>(State.OPEN);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    requireAndSetState(State.OPEN, State.CLOSED, "close an HBaseKijiTableAnnotator");
    mTable.release();
  }

  /**
   * Require that the annotator is in the given state while attempting the given action and
   * atomically set it to a new state regardless of the success of this check.
   *
   * @param required the state in which the annotator must be to pass this check.
   * @param toSet the new state to which the annotator will be set regardless of the success of this
   *     check.
   * @param attemptedAction the action being attempted which triggered this check.
   */
  private void requireAndSetState(
      final State required,
      final State toSet,
      final String attemptedAction
  ) {
    final State actual = mState.getAndSet(toSet);
    Preconditions.checkState(
        required == actual, "Cannot %s with annotator in state: %s", attemptedAction, actual);
  }

  /**
   * Require that the annotator is in the given state while attempting the given action.
   *
   * @param required the state in which the annotator must be to pass this check.
   * @param attemptedAction the action being attempted which triggered this check.
   */
  private void requireState(
      final State required,
      final String attemptedAction
  ) {
    final State actual = mState.get();
    Preconditions.checkState(
        required == actual, "Cannot %s with annotator in state: %s", attemptedAction, actual);
  }

  /**
   * Write an annotation key value to the meta table.
   *
   * @param metaTableKey meta table key of the annotation to write.
   * @param value String value of the annotation to write.
   * @throws IOException in case of an error writing to the meta table.
   */
  private void setKV(
      final String metaTableKey,
      final String value
  ) throws IOException {
    mTable.getKiji().getMetaTable().putValue(mTable.getName(), metaTableKey, Bytes.toBytes(value));
  }

  /**
   * Remove the given meta table key.
   *
   * @param metaTableKey the key to remove from the meta table.
   * @throws IOException in case of an error writing to the meta table.
   */
  private void removeKV(
      final String metaTableKey
  ) throws IOException {
    mTable.getKiji().getMetaTable().removeValues(mTable.getName(), metaTableKey);
  }

  /**
   * Get the String value of an annotation from its meta table key.
   *
   * @param metaTableKey the meta table key of the annotation to retrieve.
   * @return the value of the given annotation or null if no annotation exists.
   * @throws IOException in case of an error reading from the meta table.
   */
  private String getKV(
      final String metaTableKey
  ) throws IOException {
    try {
      return Bytes.toString(
          mTable.getKiji().getMetaTable().getValue(mTable.getName(), metaTableKey));
    } catch (IOException ioe) {
      if (ioe.getMessage().equals(String.format(
          "Could not find any values associated with table %s and key %s",
          mTable.getName(), metaTableKey))) {
        return null;
      } else {
        throw ioe;
      }
    }
  }

  /**
   * Get the keySet from the meta table for the table served by this annotator.
   *
   * @return the meta table key set for the table served by this annotator.
   * @throws IOException in case of an error reading form the meta table.
   */
  private Set<String> keySet() throws IOException {
    return mTable.getKiji().getMetaTable().keySet(mTable.getName());
  }

  /**
   * Get the KijiColumnName from the given meta table key. If the key component cannot be translated
   * to a KijiColumnName, the meta table key will be removed and this method will return null.
   *
   * @param metaTableKey key from which to exact the column name.
   * @return KijiColumnName stored in the given meta table key or null if the column name cannot be
   *     translated.
   * @throws IOException in case of an error removing a defunct key.
   */
  private KijiColumnName columnFromMetaTableKey(
      final String metaTableKey
  ) throws IOException {
    try {
      return columnFromMetaTableKey(mTable, metaTableKey);
    } catch (NoSuchColumnException e) {
      removeKV(metaTableKey);
      return null;
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Public interface
  // -----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public void setTableAnnotation(
      final String key,
      final String value
  ) throws IOException {
    requireState(State.OPEN, "set annotation");
    validateAnnotationKey(key);
    setKV(getMetaTableKey(key), value);
  }

  /** {@inheritDoc} */
  @Override
  public void setColumnAnnotation(
      final KijiColumnName column,
      final String key,
      final String value
  ) throws IOException {
    requireState(State.OPEN, "set annotation");
    validateAnnotationKey(key);
    setKV(getMetaTableKey(mTable, column, key), value);
  }

  /** {@inheritDoc} */
  @Override
  public void setTableAnnotations(
      final Map<String, String> kvs
  ) throws IOException {
    requireState(State.OPEN, "set annotation");
    for (Map.Entry<String, String> kv: kvs.entrySet()) {
      validateAnnotationKey(kv.getKey());
      setKV(getMetaTableKey(kv.getKey()), kv.getValue());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void setColumnAnnotations(
      final KijiColumnName column,
      final Map<String, String> kvs
  ) throws IOException {
    requireState(State.OPEN, "set annotation");
    for (Map.Entry<String, String> kv: kvs.entrySet()) {
      validateAnnotationKey(kv.getKey());
      setKV(getMetaTableKey(mTable, column, kv.getKey()), kv.getValue());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void removeTableAnnotation(
      final String exactKey
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    removeKV(getMetaTableKey(exactKey));
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> removeTableAnnotationsStartingWith(
      final String prefix
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Set<String> removedAnnotationKeys = Sets.newHashSet();
    for (String metaTableKey : keySet()) {
      if (isKCATableMetaTableKey(metaTableKey)) {
        final String annotationKey = keyFromMetaTableKey(metaTableKey);
        if (annotationKey.startsWith(prefix)) {
          removedAnnotationKeys.add(annotationKey);
          removeKV(metaTableKey);
        }
      }
    }
    return removedAnnotationKeys;
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> removeTableAnnotationsContaining(
      final String substring
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Set<String> removedAnnotationKeys = Sets.newHashSet();
    for (String metaTableKey : keySet()) {
      if (isKCATableMetaTableKey(metaTableKey)) {
        final String annotationKey = keyFromMetaTableKey(metaTableKey);
        if (annotationKey.contains(substring)) {
          removedAnnotationKeys.add(annotationKey);
          removeKV(metaTableKey);
        }
      }
    }
    return removedAnnotationKeys;
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> removeTableAnnotationsMatching(
      final String pattern
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Set<String> removedAnnotationKeys = Sets.newHashSet();
    for (String metaTableKey : keySet()) {
      if (isKCATableMetaTableKey(metaTableKey)) {
        final String annotationKey = keyFromMetaTableKey(metaTableKey);
        if (annotationKey.matches(pattern)) {
          removedAnnotationKeys.add(annotationKey);
          removeKV(metaTableKey);
        }
      }
    }
    return removedAnnotationKeys;
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> removeAllTableAnnotations() throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Set<String> removedAnnotationKeys = Sets.newHashSet();
    for (String metaTableKey : keySet()) {
      if (isKCATableMetaTableKey(metaTableKey)) {
        removedAnnotationKeys.add(keyFromMetaTableKey(metaTableKey));
        removeKV(metaTableKey);
      }
    }
    return removedAnnotationKeys;
  }

  /** {@inheritDoc} */
  @Override
  public void removeColumnAnnotation(
      final KijiColumnName column,
      final String exactKey
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    removeKV(getMetaTableKey(mTable, column, exactKey));
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> removeColumnAnnotationsStartingWith(
      final KijiColumnName column,
      final String prefix
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Set<String> removedAnnotationKeys = Sets.newHashSet();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)
          && Objects.equal(column, columnFromMetaTableKey(metaTableKey))) {
        final String annotationKey = keyFromMetaTableKey(metaTableKey);
        if (annotationKey.startsWith(prefix)) {
          removedAnnotationKeys.add(annotationKey);
          removeKV(metaTableKey);
        }
      }
    }
    return removedAnnotationKeys;
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> removeColumnAnnotationsContaining(
      final KijiColumnName column,
      final String substring
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Set<String> removedAnnotationKeys = Sets.newHashSet();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)
          && Objects.equal(column, columnFromMetaTableKey(metaTableKey))) {
        final String annotationKey = keyFromMetaTableKey(metaTableKey);
        if (annotationKey.contains(substring)) {
          removedAnnotationKeys.add(annotationKey);
          removeKV(metaTableKey);
        }
      }
    }
    return removedAnnotationKeys;
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> removeColumnAnnotationsMatching(
      final KijiColumnName column,
      final String pattern
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Set<String> removedAnnotationKeys = Sets.newHashSet();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)
          && Objects.equal(column, columnFromMetaTableKey(metaTableKey))) {
        final String annotationKey = keyFromMetaTableKey(metaTableKey);
        if (annotationKey.matches(pattern)) {
          removedAnnotationKeys.add(annotationKey);
          removeKV(metaTableKey);
        }
      }
    }
    return removedAnnotationKeys;
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> removeAllColumnAnnotations(
      final KijiColumnName column
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Set<String> removedAnnotationKeys = Sets.newHashSet();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)
          && Objects.equal(column, columnFromMetaTableKey(metaTableKey))) {
        removedAnnotationKeys.add(keyFromMetaTableKey(metaTableKey));
        removeKV(metaTableKey);
      }
    }
    return removedAnnotationKeys;
  }

  /** {@inheritDoc} */
  @Override
  public Set<KijiColumnName> removeColumnAnnotationsInFamily(
      final String family,
      final String exactKey
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Set<KijiColumnName> removedColumns = Sets.newHashSet();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)
          && Objects.equal(exactKey, keyFromMetaTableKey(metaTableKey))) {
        final KijiColumnName column = columnFromMetaTableKey(metaTableKey);
        if (null != column && Objects.equal(family, column.getFamily())) {
          removedColumns.add(column);
          removeKV(metaTableKey);
        }
      }
    }
    return removedColumns;
  }

  /** {@inheritDoc} */
  @Override
  public Map<KijiColumnName, Set<String>> removeColumnAnnotationsInFamilyStartingWith(
      final String family,
      final String prefix
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Map<KijiColumnName, Set<String>> removedAnnotationKeys = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)) {
        final KijiColumnName column = columnFromMetaTableKey(metaTableKey);
        if (null != column && Objects.equal(family, column.getFamily())) {
          final String annotationKey = keyFromMetaTableKey(metaTableKey);
          if (annotationKey.startsWith(prefix)) {
            final Set<String> removedKeysInColumn = removedAnnotationKeys.get(column);
            if (null != removedKeysInColumn) {
              removedKeysInColumn.add(annotationKey);
            } else {
              removedAnnotationKeys.put(column, Sets.newHashSet(annotationKey));
            }
            removeKV(metaTableKey);
          }
        }
      }
    }
    return removedAnnotationKeys;
  }

  /** {@inheritDoc} */
  @Override
  public Map<KijiColumnName, Set<String>> removeColumnAnnotationsInFamilyContaining(
      final String family,
      final String substring
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Map<KijiColumnName, Set<String>> removedAnnotationKeys = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)) {
        final KijiColumnName column = columnFromMetaTableKey(metaTableKey);
        if (null != column && Objects.equal(family, column.getFamily())) {
          final String annotationKey = keyFromMetaTableKey(metaTableKey);
          if (annotationKey.contains(substring)) {
            final Set<String> removedKeysInColumn = removedAnnotationKeys.get(column);
            if (null != removedKeysInColumn) {
              removedKeysInColumn.add(annotationKey);
            } else {
              removedAnnotationKeys.put(column, Sets.newHashSet(annotationKey));
            }
            removeKV(metaTableKey);
          }
        }
      }
    }
    return removedAnnotationKeys;
  }

  /** {@inheritDoc} */
  @Override
  public Map<KijiColumnName, Set<String>> removeColumnAnnotationsInFamilyMatching(
      final String family,
      final String pattern
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Map<KijiColumnName, Set<String>> removedAnnotationKeys = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)) {
        final KijiColumnName column = columnFromMetaTableKey(metaTableKey);
        if (null != column && Objects.equal(family, column.getFamily())) {
          final String annotationKey = keyFromMetaTableKey(metaTableKey);
          if (annotationKey.matches(pattern)) {
            final Set<String> removedKeysInColumn = removedAnnotationKeys.get(column);
            if (null != removedKeysInColumn) {
              removedKeysInColumn.add(annotationKey);
            } else {
              removedAnnotationKeys.put(column, Sets.newHashSet(annotationKey));
            }
            removeKV(metaTableKey);
          }
        }
      }
    }
    return removedAnnotationKeys;
  }

  /** {@inheritDoc} */
  @Override
  public Map<KijiColumnName, Set<String>> removeAllColumnAnnotationsInFamily(
      final String family
  ) throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Map<KijiColumnName, Set<String>> removedAnnotationKeys = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)) {
        final KijiColumnName column = columnFromMetaTableKey(metaTableKey);
        if (null != column && Objects.equal(family, column.getFamily())) {
          final Set<String> removedKeysInColumn = removedAnnotationKeys.get(column);
          if (null != removedKeysInColumn) {
            removedKeysInColumn.add(keyFromMetaTableKey(metaTableKey));
          } else {
            removedAnnotationKeys.put(column, Sets.newHashSet(keyFromMetaTableKey(metaTableKey)));
          }
          removeKV(metaTableKey);
        }
      }
    }
    return removedAnnotationKeys;
  }

  /** {@inheritDoc} */
  @Override
  public Map<KijiColumnName, Set<String>> removeAllColumnAnnotations() throws IOException {
    requireState(State.OPEN, "remove annotation");
    final Map<KijiColumnName, Set<String>> removedAnnotationKeys = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)) {
        final KijiColumnName column = columnFromMetaTableKey(metaTableKey);
        if (null != column) {
          final Set<String> removedKeysInColumn = removedAnnotationKeys.get(column);
          if (null != removedKeysInColumn) {
            removedKeysInColumn.add(keyFromMetaTableKey(metaTableKey));
          } else {
            removedAnnotationKeys.put(column, Sets.newHashSet(keyFromMetaTableKey(metaTableKey)));
          }
          removeKV(metaTableKey);
        }
      }
    }
    return removedAnnotationKeys;
  }

  /** {@inheritDoc} */
  @Override
  public String getTableAnnotation(
      final String exactKey
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    return getKV(getMetaTableKey(exactKey));
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getTableAnnotationsStartingWith(
      final String prefix
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<String, String> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCATableMetaTableKey(metaTableKey)) {
        final String annotationKey = keyFromMetaTableKey(metaTableKey);
        if (keyFromMetaTableKey(metaTableKey).startsWith(prefix)) {
          collectedAnnotations.put(annotationKey, getKV(metaTableKey));
        }
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getTableAnnotationsContaining(
      final String substring
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<String, String> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCATableMetaTableKey(metaTableKey)) {
        final String annotationKey = keyFromMetaTableKey(metaTableKey);
        if (keyFromMetaTableKey(metaTableKey).contains(substring)) {
          collectedAnnotations.put(annotationKey, getKV(metaTableKey));
        }
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getTableAnnotationsMatching(
      final String pattern
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<String, String> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCATableMetaTableKey(metaTableKey)) {
        final String annotationKey = keyFromMetaTableKey(metaTableKey);
        if (keyFromMetaTableKey(metaTableKey).matches(pattern)) {
          collectedAnnotations.put(annotationKey, getKV(metaTableKey));
        }
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getAllTableAnnotations() throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<String, String> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCATableMetaTableKey(metaTableKey)) {
        collectedAnnotations.put(keyFromMetaTableKey(metaTableKey), getKV(metaTableKey));
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public String getColumnAnnotation(
      final KijiColumnName column,
      final String exactKey
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    return getKV(getMetaTableKey(mTable, column, exactKey));
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getColumnAnnotationsStartingWith(
      final KijiColumnName column,
      final String prefix
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<String, String> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)
          && Objects.equal(column, columnFromMetaTableKey(metaTableKey))) {
        final String annotationKey = keyFromMetaTableKey(metaTableKey);
        if (annotationKey.startsWith(prefix)) {
          collectedAnnotations.put(annotationKey, getKV(metaTableKey));
        }
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getColumnAnnotationsContaining(
      final KijiColumnName column,
      final String substring
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<String, String> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)
          && Objects.equal(column, columnFromMetaTableKey(metaTableKey))) {
        final String annotationKey = keyFromMetaTableKey(metaTableKey);
        if (annotationKey.contains(substring)) {
          collectedAnnotations.put(annotationKey, getKV(metaTableKey));
        }
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getColumnAnnotationsMatching(
      final KijiColumnName column,
      final String pattern
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<String, String> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)
          && Objects.equal(column, columnFromMetaTableKey(metaTableKey))) {
        final String annotationKey = keyFromMetaTableKey(metaTableKey);
        if (annotationKey.matches(pattern)) {
          collectedAnnotations.put(annotationKey, getKV(metaTableKey));
        }
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getAllColumnAnnotations(
      final KijiColumnName column
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<String, String> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)
          && Objects.equal(column, columnFromMetaTableKey(metaTableKey))) {
        collectedAnnotations.put(keyFromMetaTableKey(metaTableKey), getKV(metaTableKey));
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public Map<KijiColumnName, String> getColumnAnnotationsInFamily(
      final String family,
      final String exactKey
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<KijiColumnName, String> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)) {
        final KijiColumnName column = columnFromMetaTableKey(metaTableKey);
        if (null != column
            && Objects.equal(family, column.getFamily())
            && Objects.equal(exactKey, keyFromMetaTableKey(metaTableKey))) {
          collectedAnnotations.put(column, getKV(metaTableKey));
        }
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public Map<KijiColumnName, Map<String, String>> getColumnAnnotationsInFamilyStartingWith(
      final String family,
      final String prefix
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<KijiColumnName, Map<String, String>> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)) {
        final KijiColumnName column =
            columnFromMetaTableKey(metaTableKey);
        if (Objects.equal(family, column.getFamily())) {
          final String annotationKey = keyFromMetaTableKey(metaTableKey);
          if (annotationKey.startsWith(prefix)) {
            final Map<String, String> existingAnnotations = collectedAnnotations.get(column);
            if (null != existingAnnotations) {
              existingAnnotations.put(annotationKey, getKV(metaTableKey));
            } else {
              final Map<String, String> newAnnotations = Maps.newHashMap();
              newAnnotations.put(annotationKey, getKV(metaTableKey));
              collectedAnnotations.put(column, newAnnotations);
            }
          }
        }
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public Map<KijiColumnName, Map<String, String>> getColumnAnnotationsInFamilyContaining(
      final String family,
      final String substring
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<KijiColumnName, Map<String, String>> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)) {
        final KijiColumnName column = columnFromMetaTableKey(metaTableKey);
        if (null != column && Objects.equal(family, column.getFamily())) {
          final String annotationKey = keyFromMetaTableKey(metaTableKey);
          if (annotationKey.contains(substring)) {
            final Map<String, String> existingAnnotations = collectedAnnotations.get(column);
            if (null != existingAnnotations) {
              existingAnnotations.put(annotationKey, getKV(metaTableKey));
            } else {
              final Map<String, String> newAnnotations = Maps.newHashMap();
              newAnnotations.put(annotationKey, getKV(metaTableKey));
              collectedAnnotations.put(column, newAnnotations);
            }
          }
        }
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public Map<KijiColumnName, Map<String, String>> getColumnAnnotationsInFamilyMatching(
      final String family,
      final String pattern
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<KijiColumnName, Map<String, String>> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)) {
        final KijiColumnName column = columnFromMetaTableKey(metaTableKey);
        if (null != column && Objects.equal(family, column.getFamily())) {
          final String annotationKey = keyFromMetaTableKey(metaTableKey);
          if (annotationKey.matches(pattern)) {
            final Map<String, String> existingAnnotations = collectedAnnotations.get(column);
            if (null != existingAnnotations) {
              existingAnnotations.put(annotationKey, getKV(metaTableKey));
            } else {
              final Map<String, String> newAnnotations = Maps.newHashMap();
              newAnnotations.put(annotationKey, getKV(metaTableKey));
              collectedAnnotations.put(column, newAnnotations);
            }
          }
        }
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public Map<KijiColumnName, Map<String, String>> getAllColumnAnnotationsInFamily(
      final String family
  ) throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<KijiColumnName, Map<String, String>> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)) {
        final KijiColumnName column = columnFromMetaTableKey(metaTableKey);
        if (null != column && Objects.equal(family, column.getFamily())) {
          final String annotationKey = keyFromMetaTableKey(metaTableKey);
          final Map<String, String> existingAnnotations = collectedAnnotations.get(column);
          if (null != existingAnnotations) {
            existingAnnotations.put(annotationKey, getKV(metaTableKey));
          } else {
            final Map<String, String> newAnnotations = Maps.newHashMap();
            newAnnotations.put(annotationKey, getKV(metaTableKey));
            collectedAnnotations.put(column, newAnnotations);
          }
        }
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public Map<KijiColumnName, Map<String, String>> getAllColumnAnnotations() throws IOException {
    requireState(State.OPEN, "get annotation");
    final Map<KijiColumnName, Map<String, String>> collectedAnnotations = Maps.newHashMap();
    for (String metaTableKey : keySet()) {
      if (isKCAColumnMetaTableKey(metaTableKey)) {
        final KijiColumnName column = columnFromMetaTableKey(metaTableKey);
        if (null != column) {
          final String annotationKey = keyFromMetaTableKey(metaTableKey);
          final Map<String, String> existingAnnotations = collectedAnnotations.get(column);
          if (null != existingAnnotations) {
            existingAnnotations.put(annotationKey, getKV(metaTableKey));
          } else {
            final Map<String, String> newAnnotations = Maps.newHashMap();
            newAnnotations.put(annotationKey, getKV(metaTableKey));
            collectedAnnotations.put(column, newAnnotations);
          }
        }
      }
    }
    return collectedAnnotations;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTable getTable() {
    return mTable;
  }
}
