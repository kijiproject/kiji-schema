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
package org.kiji.schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;

/**
 * Manages setting, retrieving, and searching key value annotations for a KijiTable.
 *
 * <p>
 *   Operations performed on multiple key value pairs are not guaranteed atomic.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
public interface KijiTableAnnotator extends Closeable {

  /**
   * Regex describing restrictions on KijiTableAnnotator keys.
   * In plain English, keys may contain one or more alphanumeric character or underscore.
   */
  String ALLOWED_ANNOTATION_KEY_PATTERN = "^[a-zA-Z0-9_]+$";

  /**
   * Set a single key value pair for the table served by this annotator.
   *
   * @param key annotation key to set. Must conform to {@link #ALLOWED_ANNOTATION_KEY_PATTERN}.
   * @param value annotation value to set.
   * @throws IOException in case of an error writing to the meta table.
   */
  void setTableAnnotation(String key, String value) throws IOException;

  /**
   * Set a single key value pair for the given column.
   *
   * @param column column on which to set the key value pair.
   * @param key annotation key to set. Must conform to {@link #ALLOWED_ANNOTATION_KEY_PATTERN}.
   * @param value annotation value to set.
   * @throws IOException in case of an error writing to the meta table.
   */
  void setColumnAnnotation(KijiColumnName column, String key, String value) throws IOException;

  /**
   * Set the given keys and values on the table served by this annotator.
   *
   * @param kvs key value mapping to set for the table served by this annotator. Keys must conform
   *     to {@link #ALLOWED_ANNOTATION_KEY_PATTERN}.
   * @throws IOException in case of an error writing to the meta table.
   */
  void setTableAnnotations(Map<String, String> kvs) throws IOException;

  /**
   * Set the given keys and values on the given column.
   *
   * @param column column on which to set the given keys and values.
   * @param kvs key value mapping to set for the given column. Keys must conform to
   *     {@link #ALLOWED_ANNOTATION_KEY_PATTERN}.
   * @throws IOException in case of an error writing to the meta table.
   */
  void setColumnAnnotations(KijiColumnName column, Map<String, String> kvs) throws IOException;

  /**
   * Remove the annotation for the given key from the table served by this annotator.
   *
   * @param exactKey key to remove from the table served by this annotator.
   * @throws IOException in case of an error writing to the meta table.
   */
  void removeTableAnnotation(String exactKey) throws IOException;

  /**
   * Remove all annotations from the table served by this annotator whose keys start with the given
   * prefix.
   *
   * @param prefix key prefix to match.
   * @return the set of removed annotation keys.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Set<String> removeTableAnnotationsStartingWith(String prefix) throws IOException;

  /**
   * Remove all annotations from the table served by this annotator whose keys contain the given
   * substring.
   *
   * @param substring key substring to match.
   * @return the set of removed annotation keys.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Set<String> removeTableAnnotationsContaining(String substring) throws IOException;

  /**
   * Remove all annotations from the table served by this annotator whose keys match the given regex
   * pattern.
   *
   * @param pattern regex pattern to match.
   * @return the set of removed annotation keys.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Set<String> removeTableAnnotationsMatching(String pattern) throws IOException;

  /**
   * Remove all annotations from the table served by this annotator. Does not remove annotations
   * from columns in the table.
   *
   * @return the set of removed annotation keys.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Set<String> removeAllTableAnnotations() throws IOException;

  /**
   * Remove the value of the given exact key from the given column.
   *
   * @param column column from which to remove an annotation.
   * @param exactKey key to remove from the given column.
   * @throws IOException in case of an error writing to the meta table.
   */
  void removeColumnAnnotation(KijiColumnName column, String exactKey) throws IOException;

  /**
   * Remove all annotations from the given column whose keys start with the given prefix.
   *
   * @param column column from which to remove annotations.
   * @param prefix key prefix to match.
   * @return the set of removed annotation keys.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Set<String> removeColumnAnnotationsStartingWith(KijiColumnName column, String prefix)
      throws IOException;

  /**
   * Remove all annotations from the given column whose keys contain the given substring.
   *
   * @param column column from which to remove annotations.
   * @param substring key substring to match.
   * @return the set of removed annotation keys.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Set<String> removeColumnAnnotationsContaining(KijiColumnName column, String substring)
      throws IOException;

  /**
   * Remove all annotations from the given column whose keys match t he given regex pattern.
   *
   * @param column column from which to remove annotations.
   * @param pattern regex pattern to match.
   * @return the set of removed annotation keys.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Set<String> removeColumnAnnotationsMatching(KijiColumnName column, String pattern)
      throws IOException;

  /**
   * Remove all annotations from the given column.
   *
   * @param column column from which to remove annotations.
   * @return the set of removed annotation keys.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Set<String> removeAllColumnAnnotations(KijiColumnName column) throws IOException;

  /**
   * Remove annotations with the given exact key from all columns in the given family.
   *
   * @param family column family from which to remove annotations.
   * @param exactKey key to remove from all columns in the given family.
   * @return the set of columns from which the exact key was removed.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Set<KijiColumnName> removeColumnAnnotationsInFamily(String family, String exactKey)
      throws IOException;

  /**
   * Remove annotations from all columns in the given family whose keys start with the given prefix.
   *
   * @param family column family from which to remove annotations.
   * @param prefix key prefix to match.
   * @return a map from column to the set of annotation keys removed from that column.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Map<KijiColumnName, Set<String>> removeColumnAnnotationsInFamilyStartingWith(
      String family, String prefix) throws IOException;

  /**
   * Remove annotations from all columns in the given family whose keys contain the given substring.
   *
   * @param family column family from which to remove annotations.
   * @param substring key substring to match.
   * @return a map from column to the set of annotation keys removed from that column.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Map<KijiColumnName, Set<String>> removeColumnAnnotationsInFamilyContaining(
      String family, String substring) throws IOException;

  /**
   * Remove annotations from all columns in the given family whose keys match the given regex
   * pattern.
   *
   * @param family column family from which to remove annotations.
   * @param pattern regex pattern to match.
   * @return a map from column to the set of annotation keys removed from that column.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Map<KijiColumnName, Set<String>> removeColumnAnnotationsInFamilyMatching(
      String family, String pattern) throws IOException;

  /**
   * Remove annotations from all columns in the given family.
   *
   * @param family column family from which to remove annotations.
   * @return a map from column to the set of annotation keys removed from that column.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Map<KijiColumnName, Set<String>> removeAllColumnAnnotationsInFamily(String family)
      throws IOException;

  /**
   * Remove all annotations from all column in the table served by this annotator.
   *
   * @return a map from column to the set of annotation keys removed from that column.
   * @throws IOException in case of an error reading or writing to the meta table.
   */
  Map<KijiColumnName, Set<String>> removeAllColumnAnnotations() throws IOException;

  /**
   * Get the value of an exact key from the table served by this annotator.
   *
   * @param exactKey exact key to retrieve.
   * @return the value of the given key on the table served by this annotator or null if the key
   *     does not exist.
   * @throws IOException in case of an error reading from the meta table.
   */
  String getTableAnnotation(String exactKey) throws IOException;

  /**
   * Get the value of all annotations on the table served by this annotator whose keys start with
   * the given prefix.
   *
   * @param prefix key prefix to match.
   * @return a mapping from keys which start with the given prefix to their values.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<String, String> getTableAnnotationsStartingWith(String prefix) throws IOException;

  /**
   * Get the value of all annotations on the table served by this annotator whose keys contain the
   * given substring.
   *
   * @param substring key substring to match.
   * @return a mapping from keys which contain the given substring to their values.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<String, String> getTableAnnotationsContaining(String substring) throws IOException;

  /**
   * Get the value of all annotations on the table served by this annotator whose keys match the
   * given regex pattern.
   *
   * @param pattern regex pattern to match.
   * @return a mapping from keys which match the given pattern to their values.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<String, String> getTableAnnotationsMatching(String pattern) throws IOException;

  /**
   * Get the value of all annotations on the table served by this annotator.
   *
   * @return a mapping from keys to values.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<String, String> getAllTableAnnotations() throws IOException;

  /**
   * Get the value of an exact key from the given column.
   *
   * @param column column from which to get the value of an annotation.
   * @param exactKey exact key to retrieve.
   * @return the value of the given key on the given column or null if the key does not exist.
   * @throws IOException in case of an error reading from the meta table.
   */
  String getColumnAnnotation(KijiColumnName column, String exactKey) throws IOException;

  /**
   * Get the value of all annotations on the given column whose keys start with the given prefix.
   *
   * @param column column from which to get annotations.
   * @param prefix key prefix to match.
   * @return a mapping from keys which start with the given prefix to their values.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<String, String> getColumnAnnotationsStartingWith(KijiColumnName column, String prefix)
      throws IOException;

  /**
   * Get the value of all annotations on the given column whose keys contain the given substring.
   *
   * @param column column from which to get annotations.
   * @param substring key substring to match.
   * @return a mapping from keys which contain the given substring to their values.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<String, String> getColumnAnnotationsContaining(KijiColumnName column, String substring)
      throws IOException;

  /**
   * Get the value of all annotations on the given column whose keys match the given regex pattern.
   *
   * @param column column from which to get annotations.
   * @param pattern regex pattern to match.
   * @return a mapping from keys which match the given pattern to their values.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<String, String> getColumnAnnotationsMatching(KijiColumnName column, String pattern)
      throws IOException;

  /**
   * Get the value of all annotations on the given column.
   *
   * @param column column from which to get annotations.
   * @return a mapping from keys to values.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<String, String> getAllColumnAnnotations(KijiColumnName column) throws IOException;

  /**
   * Get the value of all annotations on columns in the given family whose keys match the given
   * exact key.
   *
   * @param family column family from which to get annotations.
   * @param exactKey exact key to retrieve.
   * @return a mapping from columns in the given family to the value of the requested annotation for
   *     that column.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<KijiColumnName, String> getColumnAnnotationsInFamily(String family, String exactKey)
      throws IOException;

  /**
   * Get the value of all annotations on columns in the given family whose keys start with the given
   * prefix.
   *
   * @param family column family from which to get annotations.
   * @param prefix key prefix to match.
   * @return a mapping from columns in the given family to key value mappings of annotations.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<KijiColumnName, Map<String, String>> getColumnAnnotationsInFamilyStartingWith(
      String family, String prefix) throws IOException;

  /**
   * Get the value of all annotations on columns in the given family whose keys contain the given
   * substring.
   *
   * @param family column family from which to get annotations.
   * @param substring key substring to match.
   * @return a mapping from columns in the given family to key value mappings of annotations.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<KijiColumnName, Map<String, String>> getColumnAnnotationsInFamilyContaining(
      String family, String substring) throws IOException;

  /**
   * Get the value of all annotations on columns in the given family whose keys match the given
   * regex pattern.
   *
   * @param family column family from which to get annotations.
   * @param pattern regex pattern to match.
   * @return a mapping from columns in the given family to a key value mapping of annotations.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<KijiColumnName, Map<String, String>> getColumnAnnotationsInFamilyMatching(
      String family, String pattern) throws IOException;

  /**
   * Get the value of all annotations on columns in the given family.
   *
   * @param family column family from which to get annotations.
   * @return a mapping from columns in the given family to all key value annotations on that column.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<KijiColumnName, Map<String, String>> getAllColumnAnnotationsInFamily(String family)
      throws IOException;

  /**
   * Get the value of all column annotations for columns in the table served by this annotator.
   *
   * @return a mapping from column name to a mapping from key to value.
   * @throws IOException in case of an error reading from the meta table.
   */
  Map<KijiColumnName, Map<String, String>> getAllColumnAnnotations() throws IOException;

  /**
   * Get the table served by this annotator.
   *
   * @return the table served by this annotator.
   */
  KijiTable getTable();
}
