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

package org.kiji.schema.layout;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.ColumnDesc;
import org.kiji.schema.avro.ColumnNameTranslator;
import org.kiji.schema.avro.FamilyDesc;
import org.kiji.schema.avro.LocalityGroupDesc;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.Versions;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.impl.ColumnId;
import org.kiji.schema.util.FromJson;
import org.kiji.schema.util.Hasher;
import org.kiji.schema.util.KijiNameValidator;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.schema.util.ToJson;

/**
 * Layout of a Kiji table.
 *
 * <p>
 *   Kiji uses the term <i>layout</i> to describe the structure of a table.
 *   Kiji does not use the term <i>schema</i> to avoid confusion with Avro schemas or XML schemas.
 * </p>
 *
 * <p>
 *   KijiTableLayout wraps a layout descriptor represented as a
 *   {@link org.kiji.schema.avro.TableLayoutDesc TableLayoutDesc} Avro record.
 *   KijiTableLayout provides strict validation and accessors to navigate through the layout.
 * </p>
 *
 * <p>
 *   KijiTableLayouts can be created via one of two methods: from a concrete layout with
 *   {@link #newLayout(TableLayoutDesc)}, or as a layout update from a preexisting
 *   KijiTableLayout, with {@link #createUpdatedLayout(TableLayoutDesc,KijiTableLayout)}.
 *   For the format requirements of layout descriptors for these methods, see the
 *   "Layout descriptors" section below.
 * </p>
 *
 * <h1>Overall structure</h1>
 * <p>At the top-level, a table contains:
 * <ul>
 *   <li>the table name and description;</li>
 *   <li>how row keys are encoded;</li>
 *   <li>the table locality groups.</li>
 * </ul>
 * </p>
 *
 * <p>Each locality group has:
 * <ul>
 *   <li>a primary name, unique within the table, a description and some name aliases;</li>
 *   <li>whether the data is to be stored in memory or on disk;</li>
 *   <li>data retention lifetime;</li>
 *   <li>maximum number of versions to keep;</li>
 *   <li>type of compression;</li>
 *   <li>column families stored in this locality group</li>
 * </ul>
 * </p>
 *
 * <p>Each column family has:
 * <ul>
 *   <li>a primary name, globally unique within the table,
 *       a description and some name aliases;</li>
 *   <li>for map-type families, the Avro schema of the cell values;</li>
 *   <li>for group-type families, the collection of columns in the group.</li>
 * </ul>
 * </p>
 *
 * <p>Each column in a group-type family has:
 * <ul>
 *   <li>a primary name, unique within the family, a description and some name aliases;</li>
 *   <li>an Avro schema.</li>
 * </ul>
 * </p>
 *
 * <h1>Layout descriptors</h1>
 *
 * Layout descriptors are represented using
 * {@link org.kiji.schema.avro.TableLayoutDesc TableLayoutDesc} Avro records.
 * Layout descriptors come in two flavors: <i>concrete layouts</i> and <i>layout updates</i>.
 *
 * <h2><i>Concrete layout descriptors</i></h2>
 * A concrete layout descriptors is an absolute, standalone description of a table layout, which
 * does not reference or build upon any previous version of the table layout. Column IDs have
 * been assigned to all locality groups, families and columns.
 *
 * <p> Names of tables, locality groups, families and column qualifiers must be valid identifiers.
 * Name validation occurs in {@link org.kiji.schema.util.KijiNameValidator KijiNameValidator}.
 *
 * <h3>Validation rules</h3>
 *
 * <ul>
 *   <li> Table names, locality group names, family names, and column names in a group-type family
 *        must be valid identifiers (no punctuation or symbols).
 *        Note: map-type family qualifiers are free-form, but do never appear in a table layout.
 *   <li> Locality group names and aliases must be unique within the table.
 *   <li> Family names and aliases must be unique within the table.
 *   <li> Group-type family qualifiers must be unique within the family.
 * </ul>
 *
 * <h2><i>Layout update descriptors</i></h2>
 * A table layout update descriptor builds on a reference table layout, and describes layout
 * modification to apply on the reference layout.
 * The reference table layout is specified by writing the ID of the reference layout
 * ({@link TableLayoutDesc#layout_id}) into the {@link TableLayoutDesc#reference_layout}.
 * This mechanism prevents race conditions when updating the layout of a table.
 * The first layout of a newly created table has no reference layout.
 *
 * <p>During a layout update, the user may delete or declare new locality groups, families and/or
 * columns, or modify existing entities, by specifying the new layout.  Update validation rules
 * are enforced to ensure compatibility (see Validation rules for updates below).
 *
 * <p>Entities may also be renamed, as long as uniqueness requirements are met.
 * Primary name updates must be explicitly annotated by setting the {@code renamedFrom} field of
 * the entity being renamed.
 * The name of a table cannot be changed.
 *
 * <p>For example, suppose the reference layout contained one family {@code Info}, containing a
 * column {@code Name}, and the user wishes to add a new {@code Address} column to the
 * {@code Info} family.
 * To perform this update, the user would create a layout update by starting with the existing
 * layout, setting the {@code reference_layout} field to the {@code layout_id} of the
 * current layout, and adding a new {@link ColumnDesc} record describing the {@code Address}
 * column to the the {@code columns} field of the {@link FamilyDesc} for the {@code Info} family.
 *
 * <p>The result of applying a layout update on top of a concrete reference layout is a new
 * concrete layout.
 *
 * <h3> Validation rules for updates </h3>
 *
 * <p> Updates are subject to the same restrictions as concrete layout descriptors.
 * In addition:</p>
 *
 * <ul>
 *   <li> The type of a family (map-type or group-type) cannot be changed.
 *   <li> A family cannot be moved into a different locality group.
 *   <li> The encoding of Kiji cells (hash, UID, final) cannot be modified.
 *   <li> The schema of a Kiji cell can only be changed to a schema that is compatible with
 *        all the former schemas of the column. Schema compatibility requires that the new schema
 *        allows decoding all former schemas associated to the column or the map-type family.
 * </ul>
 *
 * <h1>Row keys encoding</h1>
 *
 * A row in a Kiji table is identified by its Kiji row key. Kiji row keys are converted into HBase
 * row keys according to the row key encoding specified in the table layout:
 * <ul>
 *   <li> Raw encoding: the user has direct control over the encoding of row keys in the HBase
 *        table. In other words, the HBase row key is exactly the Kiji row key. These are used
 *        when the user would like to use arrays of bytes as row keys.
 *   </li>
 *   <li> Hashed: Deprecated! The HBase row key is computed as a hash of a single String or
 *   byte array component.
 *   </li>
 *   <li> Hash-prefixed: the HBase row key is computed as the concatenation of the hash of a
 *   single String or byte array component.
 *   </li>
 *   <li> Formatted: the row key is comprised of one or more components. Each component can be
 *        a string, a number or a hash of another component. The user will specify the size
 *        of this hash. The user also specifies the actual order of the components in the key.
 *   </li>
 * </ul>
 *
 * Hashing allows to spread the rows evenly across all the regions in the table. Specifying the size
 * of the hash gives the user fine grained control of how the data will be distributed.
 *
 * <h1>Cell schema</h1>
 *
 * Kiji cells are encoded according to a schema specified via
 * {@link org.kiji.schema.avro.CellSchema CellSchema} Avro records.
 * Kiji provides various cell encoding schemes:
 * <ul>
 *   <li> Hash: each Kiji cell is encoded as a hash of the Avro schema, followed by the binary
 *        encoding of the Avro value.
 *   </li>
 *   <li> UID: each Kiji cell is encoded as the unique ID of the Avro schema, followed by the
 *        binary encoding of the Avro value.
 *   </li>
 *   <li> Final: each Kiji cell is encoded as the binary encoding of the Avro value.
 *   </li>
 * </ul>
 * See {@link org.kiji.schema.impl.AvroCellEncoder KijiCellEncoder}
 * and {@link org.kiji.schema.impl.AvroCellDecoder KijiCellDecoder}
 * for more implementation details.
 *
 * <h1>Column IDs</h1>
 *
 * Kiji allows the column names to be represented on HBase in multiple modes via
 * {@link org.kiji.schema.avro.ColumnNameTranslator ColumnNameTranslator} Avro enumeration.
 * By default we use the shortened Kiji column name translation due to space efficiency.
 * Depending on compatability requirements with other HBase tools it may be desirable to use the
 * IDENTITY or HBASE_NATIVE column name translators.
 *
 * <h2>SHORT Kiji column name translation:</h2>
 * For storage efficiency purposes, Kiji family and column names are translated into short
 * HBase column names by default.
 * This translation happens in
 *   {@link org.kiji.schema.layout.impl.hbase.ShortColumnNameTranslator ShortColumnNameTranslator}
 * and relies on
 *   {@link org.kiji.schema.layout.impl.ColumnId ColumnId}.
 * Column IDs are assigned automatically by KijiTableLayout.
 * The user may specify column IDs manually. KijiTableLayout checks the consistency of column IDs.
 *
 * <p>Column IDs cannot be changed (a column ID change is equivalent to deleting the existing column
 * and then re-creating it as a new empty column).
 *
 * <h2>IDENTITY Kiji column name translation:</h2>
 * For compatibility with other HBase tools, Kiji family and column names can be written to HBase
 * directly.
 * This translation happens in
 *   {@link org.kiji.schema.layout.impl.hbase.IdentityColumnNameTranslator}
 * In this mode:
 * <ul>
 *   <li>Kiji locality groups are translated into HBase families.</li>
 *   <li>Kiji column families and qualifiers are combined to form the HBase
 *       qualifier("family:qualifier").</li>
 * </ul>
 *
 * <h2>HBASE_NATIVE Kiji column name translation:</h2>
 * For compatibility with existing HBase tables, the notion of a Kiji locality group can be
 * ignored, mapping Kiji family and column names directly to their HBase equivalents.
 * This translation happens in
 *   {@link org.kiji.schema.layout.impl.hbase.HBaseNativeColumnNameTranslator}
 * In this mode:
 * <ul>
 *   <li>Kiji locality groups and column families are translated into HBase families.</li>
 *   <li>Additionally, Kiji locality groups must match the Kiji column families.  This has the
 *   side effect of requiring a one to one mapping between the Kiji locality groups and column
 *   families.</li>
 *   <li>Kiji column qualifiers are combined to form the HBase qualifier.</li>
 * </ul>
 *
 */
@ApiAudience.Public
@ApiStability.Evolving
public final class KijiTableLayout {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTableLayout.class);

  // ProtocolVersions specifying when different features were added to layout functionality.

  /** All layout versions must use the format 'kiji-x.y' to specify what version they use. */
  private static final String LAYOUT_PROTOCOL_NAME = "layout";

  /**
   * Returns the maximum layout version supported.
   *
   * @return the maximum layout version recognized by this version of Kiji.
   */
  public static ProtocolVersion getMaxSupportedLayoutVersion() {
    return Versions.MAX_LAYOUT_VERSION;
  }

  /**
   * Returns the minimum layout version supported.
   *
   * @return the minimum layout version recognized by this version of Kiji.
   */
  public static ProtocolVersion getMinSupportedLayoutVersion() {
    return Versions.MIN_LAYOUT_VERSION;
  }

  /** Concrete layout of a locality group. */
  @ApiAudience.Public
  public final class LocalityGroupLayout {

    /** Concrete layout of a family. */
    @ApiAudience.Public
    public final class FamilyLayout {

      /** Concrete layout of a column. */
      @ApiAudience.Public
      public final class ColumnLayout {
        /** Column layout descriptor. */
        private final ColumnDesc mDesc;

        /** Column name and aliases. */
        private final Set<String> mNames;

        /** Column ID. */
        private ColumnId mId = null;

        /**
         * Builds a new column layout instance from a descriptor.
         *
         * @param desc Column descriptor.
         * @param reference Optional reference layout, or null.
         * @throws InvalidLayoutException if the layout is invalid or inconsistent.
         */
        private ColumnLayout(ColumnDesc desc, ColumnLayout reference)
            throws InvalidLayoutException {
          mDesc = Preconditions.checkNotNull(desc);

          final Set<String> names = Sets.newHashSet();
          names.add(desc.getName());
          names.addAll(desc.getAliases());
          mNames = ImmutableSet.copyOf(names);

          if (!isValidName(desc.getName())) {
            throw new InvalidLayoutException(String.format(
                "Invalid column name: '%s'.", desc.getName()));
          }

          for (String name : mNames) {
            if (!isValidAlias(name)) {
              throw new InvalidLayoutException(String.format(
                  "Invalid column alias: '%s'.", name));
            }
          }

          if (desc.getId() > 0) {
            mId = new ColumnId(desc.getId());
          }

          if (reference != null) {
            if ((mId != null) && !mId.equals(reference.getId())) {
              throw new InvalidLayoutException(String.format(
                  "Descriptor for column '%s' has ID %s but reference ID is %s.",
                  getName(), mId, reference.getId()));
            }
            mId = reference.getId();
            desc.setId(mId.getId());
          }

          // Force validation of schema:
          final CellSchema referenceSchema =
              (null != reference) ? reference.getDesc().getColumnSchema() : null;
          validateCellSchema(mLayoutVersion, mDesc.getColumnSchema(), referenceSchema);
        }

        /** @return A copy of the Avro descriptor for this column. */
        public ColumnDesc getDesc() {
          return ColumnDesc.newBuilder(mDesc).build();
        }

        /** @return the primary name for the column. */
        public String getName() {
          return mDesc.getName();
        }

        /** @return the name and aliases for the column. */
        public Set<String> getNames() {
          return mNames;
        }

        /** @return the ID associated to this column. */
        public ColumnId getId() {
          return mId;
        }

        /**
         * Assigns the ID of this column.
         *
         * @param cid the ID of the column.
         * @return this column.
         */
        private ColumnLayout setId(ColumnId cid) {
          Preconditions.checkArgument(cid.getId() >= 1);
          Preconditions.checkState(null == mId);
          mId = cid;
          mDesc.setId(cid.getId());
          return this;
        }

        /** @return the family this column belongs to. */
        public FamilyLayout getFamily() {
          return FamilyLayout.this;
        }
      }  // class ColumnLayout

      // -------------------------------------------------------------------------------------------

      /** Family layout descriptor. */
      private final FamilyDesc mDesc;

      /** Family name and aliases. */
      private final Set<String> mNames;

      /** Columns in the family. */
      private final ImmutableList<ColumnLayout> mColumns;

      /** Map column qualifier name (no aliases) to column layout. */
      private final ImmutableMap<String, ColumnLayout> mColumnMap;

      /** Bidirectional mapping between column IDs and column names (no aliases). */
      private final BiMap<ColumnId, String> mColumnIdNameMap;

      /** Family ID. */
      private ColumnId mId = null;

      // CSOFF: MethodLengthCheck
      /**
       * Builds a new family layout instance.
       *
       * @param familyDesc Descriptor of the family.
       * @param reference Optional reference family layout, or null.
       * @throws InvalidLayoutException if the layout is invalid or inconsistent.
       */
      private FamilyLayout(FamilyDesc familyDesc, FamilyLayout reference)
          throws InvalidLayoutException {
        mDesc = Preconditions.checkNotNull(familyDesc);

        // Ensure the array of columns is mutable:
        mDesc.setColumns(Lists.newArrayList(mDesc.getColumns()));

        if (!mDesc.getColumns().isEmpty() && (null != mDesc.getMapSchema())) {
          throw new InvalidLayoutException(String.format(
              "Invalid family '%s' with both map-type and columns",
              getName()));
        }

        final Set<String> familyNames = Sets.newHashSet();
        familyNames.add(familyDesc.getName());
        familyNames.addAll(familyDesc.getAliases());
        mNames = ImmutableSet.copyOf(familyNames);

        if (!isValidName(familyDesc.getName())) {
          throw new InvalidLayoutException(String.format(
              "Invalid family name: '%s'.", familyDesc.getName()));
        }

        for (String name : mNames) {
          if (!isValidAlias(name)) {
            throw new InvalidLayoutException(String.format(
                "Invalid family alias: '%s'.", name));
          }
        }

        if (familyDesc.getId() > 0) {
          mId = new ColumnId(familyDesc.getId());
        }

        if (reference != null) {
          if ((mId != null) && !mId.equals(reference.getId())) {
            throw new InvalidLayoutException(String.format(
                "Descriptor for family '%s' has ID %s but reference ID is %s.",
                getName(), mId, reference.getId()));
          }
          mId = reference.getId();
          familyDesc.setId(mId.getId());

          // Cannot change family type (group-type vs map-type):
          if (reference.isMapType() != this.isMapType()) {
            throw new InvalidLayoutException(String.format(
                "Invalid layout update for family '%s' from reference type %s to type %s.",
                getName(),
                reference.isMapType() ? "map" : "group",
                this.isMapType() ? "map" : "group"));
          }
        }

        if (this.isMapType()) {
          // Force validation of schema:
          final CellSchema referenceSchema =
              (null != reference) ? reference.getDesc().getMapSchema() : null;
          validateCellSchema(mLayoutVersion, mDesc.getMapSchema(), referenceSchema);
        }

        // Build columns:

        /**
         * Map of columns from the reference layout.
         * Entries are removed as they are processed and linked to column descriptors.
         * At the end of the process, this map must be empty.
         */
        final BiMap<String, ColumnId> refCIdMap = (reference != null)
            ? HashBiMap.create(reference.getColumnIdNameMap().inverse())
            : HashBiMap.<String, ColumnId>create();

        final List<ColumnLayout> columns = Lists.newArrayList();
        final Map<String, ColumnLayout> columnMap = Maps.newHashMap();

        /** Map of columns in the new layout. */
        final BiMap<ColumnId, String> idMap = HashBiMap.create();

        /** Columns with no ID assigned yet. */
        final List<ColumnLayout> unassigned = Lists.newArrayList();

        final Iterator<ColumnDesc> itColumnDesc = familyDesc.getColumns().iterator();
        while (itColumnDesc.hasNext()) {
          final ColumnDesc columnDesc = itColumnDesc.next();
          final boolean isRename = (columnDesc.getRenamedFrom() != null);
          final String refCName = isRename ? columnDesc.getRenamedFrom() : columnDesc.getName();
          columnDesc.setRenamedFrom(null);
          if (isRename && (reference == null)) {
            throw new InvalidLayoutException(String.format(
                "Invalid renaming: cannot find reference family for column '%s:%s'.",
                getName(), refCName));
          }
          final ColumnLayout refCLayout =
              (reference != null) ? reference.getColumnMap().get(refCName) : null;
          if (isRename && (refCLayout == null)) {
            throw new InvalidLayoutException(String.format(
                "Invalid renaming: cannot find column '%s:%s' in reference family.",
                getName(), refCName));
          }

          final ColumnId refCId = refCIdMap.remove(refCName);

          if (columnDesc.getDelete()) {
            if (refCId == null) {
              throw new InvalidLayoutException(String.format(
                  "Deleted column '%s:%s' does not exist in reference layout.",
                  mDesc.getName(), refCName));
            }
            itColumnDesc.remove();
            continue;
          }

          final ColumnLayout cLayout = new ColumnLayout(columnDesc, refCLayout);
          columns.add(cLayout);
          for (String columnName : cLayout.getNames()) {
            if (null != columnMap.put(columnName, cLayout)) {
                throw new InvalidLayoutException(String.format(
                    "Family '%s' contains duplicate column qualifier '%s'.",
                    getName(), columnName));
            }
          }
          if (cLayout.getId() != null) {
            final String previous = idMap.put(cLayout.getId(), cLayout.getName());
            Preconditions.checkState(previous == null,
                String.format("Duplicate column ID '%s' associated to '%s' and '%s'.",
                    cLayout.getId(), cLayout.getName(), previous));
          } else {
            unassigned.add(cLayout);
          }
        }

        if (!refCIdMap.isEmpty()) {
          throw new InvalidLayoutException(String.format(
              "Descriptor for family '%s' is missing columns: %s.",
              getName(), Joiner.on(",").join(refCIdMap.keySet())));
        }

        mColumns = ImmutableList.copyOf(columns);
        mColumnMap = ImmutableMap.copyOf(columnMap);

        // Assign IDs to columns, build ID maps
        int nextColumnId = 1;
        for (ColumnLayout column : unassigned) {
          Preconditions.checkState(column.getId() == null);
          while (true) {
            final ColumnId columnId = new ColumnId(nextColumnId);
            nextColumnId += 1;
            if (!idMap.containsKey(columnId)) {
              column.setId(columnId);
              idMap.put(columnId, column.getName());
              break;
            }
          }
        }

        mColumnIdNameMap = ImmutableBiMap.copyOf(idMap);
      }

      /** @return the Avro descriptor for this family. */
      public FamilyDesc getDesc() {
        return FamilyDesc.newBuilder(mDesc).build();
      }

      /** @return the primary name for the family. */
      public String getName() {
        return mDesc.getName();
      }

      /** @return the family name and aliases. */
      public Set<String> getNames() {
        return mNames;
      }

      /** @return the column ID assigned to this family. */
      public ColumnId getId() {
        return mId;
      }

      /**
       * Assigns the ID of this family.
       *
       * @param cid the ID of the family.
       * @return this column.
       */
      private FamilyLayout setId(ColumnId cid) {
        Preconditions.checkArgument(cid.getId() >= 1);
        Preconditions.checkState(null == mId);
        mId = cid;
        mDesc.setId(cid.getId());
        return this;
      }

      /** @return the columns in this family. */
      public Collection<ColumnLayout> getColumns() {
        return mColumns;
      }

      /** @return the mapping from column names (no aliases) to column layouts. */
      public Map<String, ColumnLayout> getColumnMap() {
        return mColumnMap;
      }

      /** @return the bidirectional mapping between column names (no aliases) and IDs. */
      public BiMap<ColumnId, String> getColumnIdNameMap() {
        return mColumnIdNameMap;
      }

      /** @return the locality group this family belongs to. */
      public LocalityGroupLayout getLocalityGroup() {
        return LocalityGroupLayout.this;
      }

      /** @return whether this is a group-type family. */
      public boolean isGroupType() {
        return mDesc.getMapSchema() == null;
      }

      /** @return whether this is a map-type family. */
      public boolean isMapType() {
        return !isGroupType();
      }
    }  // class FamilyLayout

    // -------------------------------------------------------------------------------------------

    /** Locality group descriptor. */
    private final LocalityGroupDesc mDesc;

    /** Locality group name and aliases. */
    private final ImmutableSet<String> mNames;

    /** Families in the locality group. */
    private final ImmutableList<FamilyLayout> mFamilies;

    /** Map family name or alias to family layout. */
    private final ImmutableMap<String, FamilyLayout> mFamilyMap;

    /** Bidirectional mapping between family IDs and family names (no alias). */
    private final BiMap<ColumnId, String> mFamilyIdNameBiMap;

    /** Locality group ID. */
    private ColumnId mId = null;

    /**
     * Constructs a locality group layout.
     *
     * @param lgDesc Locality group descriptor.
     * @param reference Optional reference locality group, or null.
     * @throws InvalidLayoutException if the layout is invalid or inconsistent.
     */
    private LocalityGroupLayout(LocalityGroupDesc lgDesc, LocalityGroupLayout reference)
        throws InvalidLayoutException {
      mDesc = Preconditions.checkNotNull(lgDesc);

      // Ensure the array of families is mutable:
      mDesc.setFamilies(Lists.newArrayList(mDesc.getFamilies()));

      // All the recognized names for this locality group:
      final Set<String> names = Sets.newHashSet();
      names.add(lgDesc.getName());
      names.addAll(lgDesc.getAliases());
      mNames = ImmutableSet.copyOf(names);

      if (!isValidName(lgDesc.getName())) {
        throw new InvalidLayoutException(String.format(
            "Invalid locality group name: '%s'.", lgDesc.getName()));
      }

      for (String name : mNames) {
        if (!isValidAlias(name)) {
          throw new InvalidLayoutException(String.format(
              "Invalid locality group alias: '%s'.", name));
        }
      }

      if (lgDesc.getId() > 0) {
        mId = new ColumnId(lgDesc.getId());
      }

      if (mDesc.getTtlSeconds() <= 0) {
        throw new InvalidLayoutException(String.format(
            "Invalid TTL seconds for locality group '%s': TTL must be positive, got %d.",
            getName(), mDesc.getTtlSeconds()));
      }
      if (mDesc.getMaxVersions() <= 0) {
        throw new InvalidLayoutException(String.format(
            "Invalid max versions for locality group '%s': max versions must be positive, got %d.",
            getName(), mDesc.getMaxVersions()));
      }

      if (reference != null) {
        if ((mId != null) && !mId.equals(reference.getId())) {
          throw new InvalidLayoutException(String.format(
              "Descriptor for locality group '%s' has ID %s but reference ID is %s.",
              getName(), mId, reference.getId()));
        }
        mId = reference.getId();
        lgDesc.setId(mId.getId());
      }

      // Build families:

      /**
       * Map of the family IDs from the reference layout.
       * Entries are removed as they are linked to the families in the descriptor.
       * Eventually, this map must become empty.
       */
      final BiMap<String, ColumnId> refFIdMap = (reference != null)
          ? HashBiMap.create(reference.getFamilyIdNameMap().inverse())
          : HashBiMap.<String, ColumnId>create();

      final List<FamilyLayout> families = Lists.newArrayList();
      final Map<String, FamilyLayout> familyMap = Maps.newHashMap();

      /** Map of families in the new layout. */
      final BiMap<ColumnId, String> idMap = HashBiMap.create();

      /** Families with no ID assigned yet. */
      final List<FamilyLayout> unassigned = Lists.newArrayList();

      final Iterator<FamilyDesc> itFamilyDesc = lgDesc.getFamilies().iterator();
      while (itFamilyDesc.hasNext()) {
        final FamilyDesc familyDesc = itFamilyDesc.next();
        final boolean isRename = (familyDesc.getRenamedFrom() != null);
        final String refFName = isRename ? familyDesc.getRenamedFrom() : familyDesc.getName();
        familyDesc.setRenamedFrom(null);
        if (isRename && (reference == null)) {
          throw new InvalidLayoutException(String.format(
              "Invalid rename: no reference locality group '%s' for family '%s'.",
              getName(), refFName));
        }
        final FamilyLayout refFLayout =
            (reference != null) ? reference.getFamilyMap().get(refFName) : null;
        if (isRename && (refFLayout == null)) {
          throw new InvalidLayoutException(String.format(
              "Invalid rename: cannot find reference family '%s' in locality group '%s'.",
              refFName, getName()));
        }

        final ColumnId refFId = refFIdMap.remove(refFName);

        if (familyDesc.getDelete()) {
          if (refFId == null) {
            throw new InvalidLayoutException(String.format(
                "Deleted family '%s' unknown in reference locality group '%s'.",
                refFName, getName()));
          }
          itFamilyDesc.remove();
          continue;
        }

        final FamilyLayout fLayout = new FamilyLayout(familyDesc, refFLayout);
        families.add(fLayout);
        for (String familyName : fLayout.getNames()) {
          Preconditions.checkState(familyMap.put(familyName, fLayout) == null,
              "Duplicate family name: " + familyName);
        }
        if (fLayout.getId() != null) {
          final String previous = idMap.put(fLayout.getId(), fLayout.getName());
          Preconditions.checkState(previous == null,
              String.format("Duplicate family ID '%s' associated to '%s' and '%s'.",
                  fLayout.getId(), fLayout.getName(), previous));
        } else {
          unassigned.add(fLayout);
        }
      }

      if (!refFIdMap.isEmpty()) {
        throw new InvalidLayoutException(String.format(
            "Descriptor for locality group '%s' is missing families: %s",
            lgDesc.getName(), Joiner.on(",").join(refFIdMap.keySet())));
      }

      mFamilies = ImmutableList.copyOf(families);
      mFamilyMap = ImmutableMap.copyOf(familyMap);

      // Assign IDs to families:
      int nextFamilyId = 1;
      for (FamilyLayout fLayout : unassigned) {
        Preconditions.checkState(fLayout.getId() == null);
        while (true) {
          final ColumnId fId = new ColumnId(nextFamilyId);
          nextFamilyId += 1;
          if (!idMap.containsKey(fId)) {
            fLayout.setId(fId);
            idMap.put(fId, fLayout.getName());
            break;
          }
        }
      }

      mFamilyIdNameBiMap = ImmutableBiMap.copyOf(idMap);
    }

    /** @return the table layout this locality group belongs to. */
    public KijiTableLayout getTableLayout() {
      return KijiTableLayout.this;
    }

    /** @return the Avro descriptor for this locality group. */
    public LocalityGroupDesc getDesc() {
      return LocalityGroupDesc.newBuilder(mDesc).build();
    }

    /** @return the locality group primary name. */
    public String getName() {
      return mDesc.getName();
    }

    /** @return the locality group name and aliases. */
    public Set<String> getNames() {
      return mNames;
    }

    /** @return the ID associated to this locality group. */
    public ColumnId getId() {
      return mId;
    }

    /**
     * Assigns the ID of the locality group.
     *
     * @param cid the ID of the locality group.
     * @return this locality group.
     */
    private LocalityGroupLayout setId(ColumnId cid) {
      Preconditions.checkArgument(cid.getId() >= 1);
      Preconditions.checkState(null == mId);
      mId = cid;
      mDesc.setId(cid.getId());
      return this;
    }

    /** @return the families in this locality group, in no particular order. */
    public Collection<FamilyLayout> getFamilies() {
      return mFamilies;
    }

    /** @return the mapping from family names and aliases to family layouts. */
    public Map<String, FamilyLayout> getFamilyMap() {
      return mFamilyMap;
    }

    /** @return the bidirectional mapping between family names (no alias) and IDs. */
    public BiMap<ColumnId, String> getFamilyIdNameMap() {
      return mFamilyIdNameBiMap;
    }

  }  // class LocalityGroupLayout

  // -----------------------------------------------------------------------------------------------

  /** Avro record describing the table layout absolutely (no reference layout required). */
  private final TableLayoutDesc mDesc;

  /** Version of this table layout. */
  private final ProtocolVersion mLayoutVersion;

  /** Locality groups in the table, in no particular order. */
  private final ImmutableList<LocalityGroupLayout> mLocalityGroups;

  /** Map locality group name or alias to locality group layout. */
  private final ImmutableMap<String, LocalityGroupLayout> mLocalityGroupMap;

  /** Families in the table, in no particular order. */
  private final ImmutableList<FamilyLayout> mFamilies;

  /** Map family names and aliases to family layout. */
  private final ImmutableMap<String, FamilyLayout> mFamilyMap;

  /** Bidirectional map between locality group names (no alias) and IDs. */
  private final ImmutableBiMap<ColumnId, String> mLocalityGroupIdNameMap;

  /** All primary column names in the table (including names for map-type families). */
  private final ImmutableSet<KijiColumnName> mColumnNames;

  /**
   * Optional schema table that allows resolution of Avro schemas.
   * The schema table is injected in the CellSpec instances created from this layout.
   */
  private KijiSchemaTable mSchemaTable;

  /**
   * Ensure a row key format (version 1) specified in a layout file is sane.
   * @param format The RowKeyFormat created from the layout file for a table.
   * @throws InvalidLayoutException If the format is invalid.
   */
  private void isValidRowKeyFormat1(RowKeyFormat format) throws InvalidLayoutException {
    RowKeyEncoding rowKeyEncoding = format.getEncoding();
    if (rowKeyEncoding != RowKeyEncoding.RAW
        && rowKeyEncoding != RowKeyEncoding.HASH
        && rowKeyEncoding != RowKeyEncoding.HASH_PREFIX) {
      throw new InvalidLayoutException("RowKeyFormat only supports encodings"
          + "of type RAW, HASH and HASH_PREFIX. Use RowKeyFormat2 instead");
    }
    if (rowKeyEncoding == RowKeyEncoding.HASH || rowKeyEncoding == RowKeyEncoding.HASH_PREFIX) {
      if (format.getHashSize() < 0 || format.getHashSize() > Hasher.HASH_SIZE_BYTES) {
        throw new InvalidLayoutException("HASH or HASH_PREFIX row key formats require hash size"
            + "to be between 1 and " + Hasher.HASH_SIZE_BYTES);
      }
    }
  }

  /**
   * Ensure a row key format (version 2) specified in a layout file is sane.
   * @param format The RowKeyFormat2 created from the layout file for a table.
   * @throws InvalidLayoutException If the format is invalid.
   */
  private void isValidRowKeyFormat2(RowKeyFormat2 format) throws InvalidLayoutException {
    // RowKeyFormat2 can only contain RAW or FORMATTED encoding types.
    if (format.getEncoding() != RowKeyEncoding.RAW
        && format.getEncoding() != RowKeyEncoding.FORMATTED) {
      throw new InvalidLayoutException("RowKeyFormat2 only supports RAW or FORMATTED encoding."
          + "Found " + format.getEncoding().name());
    }

    // For RAW encoding, ignore the rest of the fields.
    if (format.getEncoding() == RowKeyEncoding.RAW) {
      return;
    }

    // At least one primitive component.
    if (format.getComponents().size() <= 0) {
      throw new InvalidLayoutException("At least 1 component is required in row key format.");
    }

    if (format.getSalt() == null) {
      // SCHEMA-489. The Avro decoder should replace this with a non-null HashSpec object,
      // but check here for paranoia.
      throw new InvalidLayoutException(
          "Null values for RowKeyFormat2.salt are only allowed for RAW encoding.");
    }

    // Nullable index cannot be the first element or anything greater
    // than the components length (number of components).
    if (format.getNullableStartIndex() <= 0
        || format.getNullableStartIndex() > format.getComponents().size()) {
      throw new InvalidLayoutException("Invalid index for nullable component. The second component"
          + " onwards can be set to null.");
    }

    // Range scan index cannot be the first element or anything greater
    // than the components length (number of components).
    if (format.getRangeScanStartIndex() <= 0
        || format.getRangeScanStartIndex() > format.getComponents().size()) {
      throw new InvalidLayoutException("Invalid range scan index. Range scans are supported "
          + "starting with the second component.");
    }

    // If suppress_key_materialization is true, range scans are impossible, as the
    // key components will not be stored.
    if (format.getSalt().getSuppressKeyMaterialization()
        && format.getRangeScanStartIndex() != format.getComponents().size()) {
      throw new InvalidLayoutException("Range scans are not supported if "
          + "suppress_key_materialization is true. Please set range_scan_start_index "
          + "to components.size");
    }

    Set<String> nameset = new HashSet<String>();
    for (RowKeyComponent component: format.getComponents()) {
      // ensure names are valid "[a-zA-Z_][a-zA-Z0-9_]*"
      if (!isValidName(component.getName())) {
        throw new InvalidLayoutException("Names should begin with a letter followed by a "
            + "combination of letters, numbers and underscores.");
      }
      nameset.add(component.getName());
    }

    // repeated component names
    if (nameset.size() != format.getComponents().size()) {
      throw new InvalidLayoutException("Component name already used.");
    }

    // hash size invalid
    if (format.getSalt().getHashSize() <= 0
        || format.getSalt().getHashSize() > Hasher.HASH_SIZE_BYTES) {
      throw new InvalidLayoutException("Valid hash sizes are between 1 and "
          + Hasher.HASH_SIZE_BYTES);
    }
  }

  /**
   * Computes the effective ProtocolVersion from a layout version string.
   *
   * <p> Normalizes kiji-1.0 into layout-1.0.0 </p>
   *
   * @param version Layout version string.
   * @return the effective layout ProtocolVersion.
   */
  private static ProtocolVersion computeLayoutVersion(String version) {
    final ProtocolVersion pversion = ProtocolVersion.parse(version);
    if (Objects.equal(pversion, Versions.LAYOUT_KIJI_1_0_0_DEPRECATED)) {
      // Deprecated "kiji-1.0" is compatible with "layout-1.0.0"
      return Versions.LAYOUT_1_0_0;
    } else {
      return pversion;
    }
  }

  // CSOFF: MethodLengthCheck
  /**
   * Constructs a KijiTableLayout from an Avro descriptor and an optional reference layout.
   *
   * @param desc Avro layout descriptor (relative to the reference layout).
   * @param reference Optional reference layout, or null.
   * @throws InvalidLayoutException if the descriptor is invalid or inconsistent wrt reference.
   */
  private KijiTableLayout(TableLayoutDesc desc, KijiTableLayout reference)
      throws InvalidLayoutException {
    // Deep-copy the descriptor to prevent mutating a parameter:
    mDesc = TableLayoutDesc.newBuilder(Preconditions.checkNotNull(desc)).build();

    // Ensure the array of locality groups is mutable:
    mDesc.setLocalityGroups(Lists.newArrayList(mDesc.getLocalityGroups()));

    // Check that the version specified in the layout matches the features used.
    // Any compatibility checks belong in this section.
    mLayoutVersion = computeLayoutVersion(mDesc.getVersion());

    if (!Objects.equal(LAYOUT_PROTOCOL_NAME, mLayoutVersion.getProtocolName())) {
      final String exceptionMessage;
      if (Objects.equal(
          Versions.LAYOUT_KIJI_1_0_0_DEPRECATED.getProtocolName(),
          mLayoutVersion.getProtocolName())) {
        // Warn the user if they tried a version number like 'kiji-0.9' or 'kiji-1.1'.
        exceptionMessage =
            String.format("Deprecated layout version protocol '%s' only valid for version '%s',"
                + " but received version '%s'. You should specify a layout version protocol"
                + " as '%s-x.y', not '%s-x.y'.",
                Versions.LAYOUT_KIJI_1_0_0_DEPRECATED.getProtocolName(),
                Versions.LAYOUT_KIJI_1_0_0_DEPRECATED,
                mLayoutVersion,
                LAYOUT_PROTOCOL_NAME,
                Versions.LAYOUT_KIJI_1_0_0_DEPRECATED.getProtocolName());
      } else {
        exceptionMessage = String.format("Invalid version protocol: '%s'. Expected '%s'.",
            mLayoutVersion.getProtocolName(),
            LAYOUT_PROTOCOL_NAME);
      }
      throw new InvalidLayoutException(exceptionMessage);
    }

    if (Versions.MAX_LAYOUT_VERSION.compareTo(mLayoutVersion) < 0) {
      throw new InvalidLayoutException("The maximum layout version we support is "
          + Versions.MAX_LAYOUT_VERSION + "; this layout requires " + mLayoutVersion);
    } else if (Versions.MIN_LAYOUT_VERSION.compareTo(mLayoutVersion) > 0) {
      throw new InvalidLayoutException("The minimum layout version we support is "
          + Versions.MIN_LAYOUT_VERSION + "; this layout requires " + mLayoutVersion);
    }

    // max_filesize and memstore_flushsize were introduced in version 1.2.
    if (Versions.BLOCK_SIZE_LAYOUT_VERSION.compareTo(mLayoutVersion) > 0) {
      if (mDesc.getMaxFilesize() != null) {
        // Cannot use max_filesize if this is the case.
        throw new InvalidLayoutException(
          "Support for specifying max_filesize begins with layout version "
            + Versions.BLOCK_SIZE_LAYOUT_VERSION.toString());
      }

      if (mDesc.getMemstoreFlushsize() != null) {
        // Cannot use memstore_flushsize if this is the case.
        throw new InvalidLayoutException(
          "Support for specifying memstore_flushsize begins with layout version "
            + Versions.BLOCK_SIZE_LAYOUT_VERSION);
      }
    } else {
      if (mDesc.getMaxFilesize() != null && mDesc.getMaxFilesize() <= 0) {
        throw new InvalidLayoutException("max_filesize must be greater than 0");
      }

      if (mDesc.getMemstoreFlushsize() != null && mDesc.getMemstoreFlushsize() <= 0) {
        throw new InvalidLayoutException("memstore_flushsize must be greater than 0");
      }
    }

    // Ability to configure column name translation was introduced in version 1.5
    if (Versions.CONFIGURE_COLUMN_NAME_TRANSLATION_VERSION.compareTo(mLayoutVersion) > 0) {
      if (mDesc.getColumnNameTranslator() != ColumnNameTranslator.SHORT) {
        throw new InvalidLayoutException(
            "Support for specifiying non-short column name translators begins with layout version "
            + Versions.CONFIGURE_COLUMN_NAME_TRANSLATION_VERSION);
      }
    }

    // Composite keys and RowKeyFormat2 was introduced in version 1.1.
    if (Versions.RKF2_LAYOUT_VERSION.compareTo(mLayoutVersion) > 0
         && mDesc.getKeysFormat() instanceof RowKeyFormat2) {
      // Cannot use RowKeyFormat2 if this is the case.
      throw new InvalidLayoutException(
          "Support for specifying keys_format as a RowKeyFormat2 begins with layout version "
          + Versions.RKF2_LAYOUT_VERSION);
    }

    if (!isValidName(getName())) {
      throw new InvalidLayoutException(String.format("Invalid table name: '%s'.", getName()));
    }

    if (reference != null) {
      if (!getName().equals(reference.getName())) {
        throw new InvalidLayoutException(String.format(
            "Invalid layout update: layout name '%s' does not match reference layout name '%s'.",
            getName(), reference.getName()));
      }

      if (!mDesc.getKeysFormat().equals(reference.getDesc().getKeysFormat())) {
        throw new InvalidLayoutException(String.format(
            "Invalid layout update from reference row keys format '%s' to row keys format '%s'.",
            reference.getDesc().getKeysFormat(), mDesc.getKeysFormat()));
      }
    }

    // Layout ID:
    if (mDesc.getLayoutId() == null) {
      try {
        final long refLayoutId =
            (reference == null) ? 0 : Long.parseLong(reference.getDesc().getLayoutId());
        final long layoutId = refLayoutId + 1;
        mDesc.setLayoutId(Long.toString(layoutId));
      } catch (NumberFormatException nfe) {
        throw new InvalidLayoutException(String.format(
            "Reference layout for table '%s' has an invalid layout ID: '%s'",
            getName(), reference.getDesc().getLayoutId()));
      }
    }

    if (mDesc.getKeysFormat() instanceof RowKeyFormat) {
      isValidRowKeyFormat1((RowKeyFormat) mDesc.getKeysFormat());
    } else if (mDesc.getKeysFormat() instanceof RowKeyFormat2) {
      // Check validity of row key format.
      isValidRowKeyFormat2((RowKeyFormat2) mDesc.getKeysFormat());
    }

    // Build localities:

    /**
     * Reference map from locality group name to locality group ID.
     * Entries are removed as we process locality group descriptors in the new layout.
     * At the end of the process, this map must be empty.
     */
    final BiMap<String, ColumnId> refLGIdMap = (reference == null)
        ? HashBiMap.<String, ColumnId>create()
        : HashBiMap.create(reference.mLocalityGroupIdNameMap.inverse());

    /** Map of locality groups in the new layout. */
    final List<LocalityGroupLayout> localityGroups = Lists.newArrayList();
    final Map<String, LocalityGroupLayout> lgMap = Maps.newHashMap();
    final BiMap<ColumnId, String> idMap = HashBiMap.create();

    /** Locality group with no ID assigned yet. */
    final List<LocalityGroupLayout> unassigned = Lists.newArrayList();

    /** All the families in the table. */
    final List<FamilyLayout> families = Lists.newArrayList();

    /** Map from family name or alias to family layout. */
    final Map<String, FamilyLayout> familyMap = Maps.newHashMap();

    /** All primary column names (including map-type families). */
    final Set<KijiColumnName> columnNames = Sets.newTreeSet();

    final Map<KijiColumnName, ColumnLayout> columnMap = Maps.newHashMap();

    final Iterator<LocalityGroupDesc> itLGDesc = mDesc.getLocalityGroups().iterator();
    while (itLGDesc.hasNext()) {
      final LocalityGroupDesc lgDesc = itLGDesc.next();
      final boolean isRename = (lgDesc.getRenamedFrom() != null);
      final String refLGName = isRename ? lgDesc.getRenamedFrom() : lgDesc.getName();
      lgDesc.setRenamedFrom(null);
      if (isRename && (reference == null)) {
        throw new InvalidLayoutException(String.format(
            "Invalid rename: no reference table layout for locality group '%s'.", refLGName));
      }
      final LocalityGroupLayout refLGLayout =
          (reference != null) ? reference.mLocalityGroupMap.get(refLGName) : null;
      if (isRename && (refLGLayout == null)) {
        throw new InvalidLayoutException(String.format(
            "Invalid rename: cannot find reference locality group '%s'.", refLGName));
      }

      final ColumnId refLGId = refLGIdMap.remove(refLGName);

      if (lgDesc.getDelete()) {
        // This locality group is deleted:
        if (refLGId == null) {
          throw new InvalidLayoutException(String.format(
              "Attempting to delete locality group '%s' unknown in reference layout.",
              refLGName));
        }
        itLGDesc.remove();
        continue;
      }

      // BloomType, block_size were introduced in version 1.2.
      if (Versions.BLOCK_SIZE_LAYOUT_VERSION.compareTo(mLayoutVersion) > 0) {
        if (lgDesc.getBlockSize() != null) {
          // Cannot use max_filesize if this is the case.
          throw new InvalidLayoutException(
            "Support for specifying block_size begins with layout version "
              + Versions.BLOCK_SIZE_LAYOUT_VERSION);
        }
        if (lgDesc.getBloomType() != null) {
          // Cannot use bloom_type if this is the case.
          throw new InvalidLayoutException(
            "Support for specifying bloom_type begins with layout version "
              + Versions.BLOCK_SIZE_LAYOUT_VERSION);
        }
      } else {
        if (lgDesc.getBlockSize() != null && lgDesc.getBlockSize() <= 0) {
          throw new InvalidLayoutException("block_size must be greater than 0");
        }
      }

      final LocalityGroupLayout lgLayout = new LocalityGroupLayout(lgDesc, refLGLayout);
      localityGroups.add(lgLayout);
      for (String lgName : lgLayout.getNames()) {
        Preconditions.checkState(lgMap.put(lgName, lgLayout) == null,
            "Duplicate locality group name: " + lgName);
      }

      if (lgLayout.getId() != null) {
        final String previous = idMap.put(lgLayout.getId(), lgLayout.getName());
        Preconditions.checkState(previous == null,
            String.format("Duplicate locality group ID '%s' associated to '%s' and '%s'.",
                lgLayout.getId(), lgLayout.getName(), previous));
      } else {
        unassigned.add(lgLayout);
      }

      families.addAll(lgLayout.getFamilies());
      for (FamilyLayout familyLayout : lgLayout.getFamilies()) {
        for (String familyName : familyLayout.getNames()) {
          if (null != familyMap.put(familyName, familyLayout)) {
              throw new InvalidLayoutException(String.format(
                  "Layout for table '%s' contains duplicate family name '%s'.",
                  getName(), familyName));
          }
        }

        if (familyLayout.isMapType()) {
          Preconditions.checkState(
              columnNames.add(KijiColumnName.create(familyLayout.getName(), null)));
        }

        for (ColumnLayout columnLayout: familyLayout.getColumns()) {
          for (String columnName : columnLayout.getNames()) {
            final KijiColumnName column = KijiColumnName.create(familyLayout.getName(), columnName);
            if (null != columnMap.put(column, columnLayout)) {
              throw new InvalidLayoutException(String.format(
                  "Layout for table '%s' contains duplicate column '%s'.",
                  getName(), column));
            }
          }
          Preconditions.checkState(columnNames.add(
              KijiColumnName.create(familyLayout.getName(), columnLayout.getName())));
        }
      }
    }

    if (!refLGIdMap.isEmpty()) {
      throw new InvalidLayoutException(String.format(
          "Missing descriptor(s) for locality group(s): %s.",
          Joiner.on(",").join(refLGIdMap.keySet())));
    }

    mLocalityGroups = ImmutableList.copyOf(localityGroups);
    mLocalityGroupMap = ImmutableMap.copyOf(lgMap);

    mFamilies = ImmutableList.copyOf(families);
    mFamilyMap = ImmutableMap.copyOf(familyMap);

    mColumnNames = ImmutableSet.copyOf(columnNames);

    // Assign IDs to locality groups:
    int nextColumnId = 1;
    for (LocalityGroupLayout localityGroup : unassigned) {
      Preconditions.checkState(localityGroup.getId() == null);
      while (true) {
        final ColumnId columnId = new ColumnId(nextColumnId);
        nextColumnId += 1;
        if (!idMap.containsKey(columnId)) {
          localityGroup.setId(columnId);
          idMap.put(columnId, localityGroup.getName());
          break;
        }
      }
    }

    mLocalityGroupIdNameMap = ImmutableBiMap.copyOf(idMap);
  }
  // CSON: MethodLengthCheck

  /**
   * Returns the Avro descriptor for this table layout.
   * @return the Avro descriptor for this table layout.
   */
  public TableLayoutDesc getDesc() {
    return TableLayoutDesc.newBuilder(mDesc).build();
  }

  /**
   * Returns the table name.
   * @return the table name.
   */
  public String getName() {
    return mDesc.getName();
  }

  /**
   * Returns the locality groups in the table, in no particular order.
   * @return the locality groups in the table, in no particular order.
   */
  public Collection<LocalityGroupLayout> getLocalityGroups() {
    return mLocalityGroups;
  }

  /**
   * Returns the bidirectional mapping between locality group names (no alias) and IDs.
   * @return the bidirectional mapping between locality group names (no alias) and IDs.
   */
  public BiMap<ColumnId, String> getLocalityGroupIdNameMap() {
    return mLocalityGroupIdNameMap;
  }

  /**
   * Returns the map from locality group names and aliases to layouts.
   * @return the map from locality group names and aliases to layouts.
   */
  public Map<String, LocalityGroupLayout> getLocalityGroupMap() {
    return mLocalityGroupMap;
  }

  /**
   * Returns the map from locality group names and aliases to layouts.
   * @return the map from locality group names and aliases to layouts.
   */
  public Map<String, FamilyLayout> getFamilyMap() {
    return mFamilyMap;
  }

  /**
   * Returns all the families in the table, in no particular order.
   * @return all the families in the table, in no particular order.
   */
  public Collection<FamilyLayout> getFamilies() {
    return mFamilies;
  }

  /**
   * Returns all the primary column names in the table, including map-type families.
   * @return all the primary column names in the table, including map-type families.
   */
  public Set<KijiColumnName> getColumnNames() {
    return mColumnNames;
  }

  /**
   * Reports the raw specification record for the specified column.
   *
   * <p> Note: in most cases, you should use {@link #getCellSpec(KijiColumnName)}. </p>
   *
   * @param columnName Column to reports the raw specification record of.
   * @return the raw specification record for the specified column.
   * @throws NoSuchColumnException if the column does not exist.
   */
  public CellSchema getCellSchema(KijiColumnName columnName) throws NoSuchColumnException {
    final FamilyLayout fLayout = mFamilyMap.get(columnName.getFamily());
    if (fLayout == null) {
      throw new NoSuchColumnException(String.format(
          "Table '%s' has no family '%s'.", getName(), columnName.getFamily()));
    }
    if (fLayout.isMapType()) {
      return CellSchema.newBuilder(fLayout.getDesc().getMapSchema()).build();
    }

    // Group-type family:
    Preconditions.checkArgument(columnName.isFullyQualified(),
        String.format("Cannot get CellFormat for entire group-type family: '%s'.", columnName));
    final FamilyLayout.ColumnLayout cLayout =
        fLayout.getColumnMap().get(columnName.getQualifier());
    if (cLayout == null) {
      throw new NoSuchColumnException(String.format(
          "Table '%s' has no column '%s'.", getName(), columnName));
    }
    return CellSchema.newBuilder(cLayout.getDesc().getColumnSchema()).build();
  }

  /**
   * Reports the Avro schema of the specified column.
   *
   * <p> This method will return null in most cases on table with layout validation enabled. </p>
   *
   * @param columnName Column name.
   * @return the Avro schema of the column.
   * @throws InvalidLayoutException if the layout is invalid.
   * @throws NoSuchColumnException if the column does not exist.
   * @deprecated With schema validation and layout 1.3, there is no single Avro schema associated
   *     with a column anymore.
   *     Use {@link #getCellSpec(KijiColumnName)} to obtain further details about a column.
   */
  @Deprecated
  public Schema getSchema(KijiColumnName columnName)
      throws InvalidLayoutException, NoSuchColumnException {
    return CellSpec.readAvroSchema(getCellSchema(columnName));
  }

  /**
   * Reports the cell format for the specified column.
   *
   * @param column Column name.
   * @return the cell format for the column.
   * @throws NoSuchColumnException if the column does not exist.
   * @deprecated Use {@link #getCellSpec(KijiColumnName)} to obtain further details about a column.
   */
  @Deprecated
  public SchemaStorage getCellFormat(KijiColumnName column) throws NoSuchColumnException {
    return getCellSchema(column).getStorage();
  }

  /**
   * Reports the specification of the specified column.
   *
   * @param column Column to report the specification of.
   * @return the specification for the specified column.
   * @throws IOException on I/O error.
   */
  public CellSpec getCellSpec(KijiColumnName column) throws IOException {
    return CellSpec.fromCellSchema(getCellSchema(column))
        .setSchemaTable(mSchemaTable);
  }

  /**
   * Reports whether a column exists.
   *
   * @param column Column name.
   * @return whether the specified column exists.
   */
  public boolean exists(KijiColumnName column) {
    final FamilyLayout fLayout = mFamilyMap.get(column.getFamily());
    if (fLayout == null) {
      // Family does not exist:
      return false;
    }

    if (fLayout.isMapType()) {
      // This is a map-type family, we don't need to validate the qualifier:
      return true;
    }

    // This is a group-type family:
    if (!column.isFullyQualified()) {
      // No column qualifier, the group-type family exists:
      return true;
    }

    // Validate the qualifier:
    return fLayout.getColumnMap().containsKey(column.getQualifier());
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof KijiTableLayout)) {
      return false;
    }
    final KijiTableLayout otherLayout = (KijiTableLayout) other;
    return getDesc().equals(otherLayout.getDesc());
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return getDesc().hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    try {
      return ToJson.toJsonString(mDesc);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Binds this table layout to the specified schema table.
   *
   * <p> Once set, the schema table bound to a table layout cannot be modified. </p>
   *
   * @param schemaTable Avro schema table to bind this layout to.
   * @return this layout.
   */
  public KijiTableLayout setSchemaTable(KijiSchemaTable schemaTable) {
    Preconditions.checkState(mSchemaTable == null);
    mSchemaTable = schemaTable;
    return this;
  }

  /**
   * Returns the schema table bound to this table layout. Null means unbound.
   * @return the schema table bound to this table layout. Null means unbound.
   */
  public KijiSchemaTable getSchemaTable() {
    return mSchemaTable;
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Validates a name (table name, locality group name, family name, or column name).
   *
   * @param name The name to validate.
   * @return whether the name is valid.
   */
  private static boolean isValidName(String name) {
    return KijiNameValidator.isValidLayoutName(name);
  }

  /**
   * Validates an alias (table name, locality group name, family name, or column name).
   *
   * @param alias The alias to validate.
   * @return whether the alias is valid.
   */
  private static boolean isValidAlias(String alias) {
    return KijiNameValidator.isValidAlias(alias);
  }

  /**
   * Validates a cell schema descriptor.
   *
   * Ignores failures due to specific Avro record classes not being present on the classpath.
   *
   * @param layoutVersion Version of the new table layout.
   * @param schema New cell schema descriptor.
   * @param reference Reference cell schema descriptor, or null.
   * @throws InvalidLayoutException if the cell schema descriptor is invalid
   *     or incompatible with the reference cell schema.
   */
  private static void validateCellSchema(
      ProtocolVersion layoutVersion,
      CellSchema schema,
      CellSchema reference)
      throws InvalidLayoutException {

    switch (schema.getType()) {
      case INLINE:
      case CLASS:
      case COUNTER:
        // Nothing to validate
        break;
      case AVRO: {
        if (layoutVersion.compareTo(Versions.LAYOUT_VALIDATION_VERSION) < 0) {
          throw new InvalidLayoutException(String.format(
              "Cell type %s requires table layout version >= %s, "
              + "got version %s in cell specification %s.",
              schema.getType(),
              Versions.LAYOUT_VALIDATION_VERSION,
              layoutVersion,
              schema));
        }
        break;
      }
      case RAW_BYTES: {
        if (layoutVersion.compareTo(Versions.RAW_BYTES_CELL_ENCODING_VERSION) < 0) {
          throw new InvalidLayoutException(String.format(
              "Cell type %s requires table layout version >= %s, "
              + "got version %s in cell specification %s.",
              schema.getType(),
              Versions.RAW_BYTES_CELL_ENCODING_VERSION,
              layoutVersion,
              schema));
        }
        break;
      }
      case PROTOBUF: {
        if (layoutVersion.compareTo(Versions.PROTOBUF_CELL_ENCODING_VERSION) < 0) {
          throw new InvalidLayoutException(String.format(
              "Cell type %s requires table layout version >= %s, "
              + "got version %s in cell specification %s.",
              schema.getType(),
              Versions.PROTOBUF_CELL_ENCODING_VERSION,
              layoutVersion,
              schema));
        }
        break;
      }
      default:
        throw new InternalKijiError("Unhandled cell type: " + schema);
    }

    // Validate Avro schema through loading and parsing:
    try {
      CellSpec.readAvroSchema(schema);
    } catch (SchemaClassNotFoundException scnfe) {
      LOG.debug(String.format("Avro schema class '%s' not found.", schema.getValue()));
    }

    // Final schema storage is only valid with counters and inline schema:
    if (schema.getStorage() == SchemaStorage.FINAL) {
      switch (schema.getType()) {
      case INLINE:
      case COUNTER:
        break;
      default:
        throw new InvalidLayoutException(String.format(
            "Invalid final column schema: %s.", schema));
      }
    }

    // Counters require schema storage final:
    if (schema.getType() == SchemaType.COUNTER) {
      if (schema.getStorage() != SchemaStorage.FINAL) {
        throw new InvalidLayoutException(String.format(
            "Invalid counter schema, storage must be final: %s.", schema));
      }
    }

    if (null != reference) {
      // Schema storage cannot change:
      if (schema.getStorage() != reference.getStorage()) {
        throw new InvalidLayoutException(String.format(
            "Cell schema storage cannot be modified from %s to %s.",
            reference, schema));
      }

      // Final schema cannot change:
      if ((reference.getStorage() == SchemaStorage.FINAL)
          && !Objects.equal(schema.getValue(), reference.getValue())) {
        throw new InvalidLayoutException(String.format(
            "Final column schema cannot be modified from %s to %s.",
            reference, schema));
      }

      // Counter is forever:
      if ((reference.getType() == SchemaType.COUNTER) ^ (schema.getType() == SchemaType.COUNTER)) {
        throw new InvalidLayoutException(String.format(
            "Column schema cannot be modified from %s to %s.",
            reference, schema));
      }
    }
  }

  /**
   * Loads a table layout from the specified resource as JSON.
   *
   * @param resource Path of the resource containing the JSON layout description.
   * @return the parsed table layout.
   * @throws IOException on I/O error.
   */
  public static KijiTableLayout createFromEffectiveJsonResource(String resource)
      throws IOException {
    return createFromEffectiveJson(KijiTableLayout.class.getResourceAsStream(resource));
  }

  /**
   * Loads a table layout from the specified JSON text.
   *
   * @param istream Input stream containing the JSON text.
   * @return the parsed table layout.
   * @throws IOException on I/O error.
   */
  public static KijiTableLayout createFromEffectiveJson(InputStream istream) throws IOException {
    try {
      final TableLayoutDesc desc = readTableLayoutDescFromJSON(istream);
      final KijiTableLayout layout = new KijiTableLayout(desc, null);
      return layout;
    } finally {
      ResourceUtils.closeOrLog(istream);
    }
  }

  /**
   * Creates and returns a new KijiTableLayout instance as specified by an Avro TableLayoutDesc
   * description record.
   *
   * @param layout The Avro TableLayoutDesc descriptor record that describes the actual layout.
   * of the table.
   * @return A new table layout.
   * @throws InvalidLayoutException If the descriptor is invalid.
   */
  public static KijiTableLayout newLayout(TableLayoutDesc layout) throws InvalidLayoutException {
    return new KijiTableLayout(layout, null);
  }

  /**
   * Creates and returns a new KijiTableLayout instance as specified by composing updates described
   * by a {@link org.kiji.schema.avro.TableLayoutDesc TableLayoutDesc} with the original
   * KijiTableLayout.  See {@link org.kiji.schema.layout.KijiTableLayout KijiTableLayout}
   * for what can and cannot be updated.
   *
   * @param updateLayoutDesc The Avro TableLayoutDesc descriptor record that describes
   * the layout update.
   * @param oldLayout The old table layout.
   * @return A new table layout.
   * @throws InvalidLayoutException If the descriptor is invalid or inconsistent wrt reference.
   */
  public static KijiTableLayout createUpdatedLayout(
      TableLayoutDesc updateLayoutDesc, KijiTableLayout oldLayout) throws InvalidLayoutException {
    return new KijiTableLayout(updateLayoutDesc, oldLayout);
  }

  /**
   * Reads a table layout descriptor from its JSON serialized form.
   *
   * @param istream JSON input stream.
   * @return the decoded table layout descriptor.
   * @throws IOException on I/O error.
   */
  public static TableLayoutDesc readTableLayoutDescFromJSON(InputStream istream)
      throws IOException {
    final String json = IOUtils.toString(istream);
    final TableLayoutDesc desc =
        (TableLayoutDesc) FromJson.fromJsonString(json, TableLayoutDesc.SCHEMA$);
    return desc;
  }

  /**
   * Find the encoding of the row key given the format.
   *
   * @param rowKeyFormat Format of row keys of type RowKeyFormat or RowKeyFormat2.
   * @return The specific row key encoding, e.g. RAW, HASH, etc.
   */
  public static RowKeyEncoding getEncoding(Object rowKeyFormat) {
    if (rowKeyFormat instanceof RowKeyFormat) {
      return ((RowKeyFormat) rowKeyFormat).getEncoding();
    } else if (rowKeyFormat instanceof RowKeyFormat2) {
      return ((RowKeyFormat2) rowKeyFormat).getEncoding();
    } else {
      throw new RuntimeException("Unsupported Row Key Format");
    }
  }

  /**
   * Get the hash size for a given row key format.
   *
   * @param rowKeyFormat Format of row keys of type RowKeyFormat or RowKeyFormat2.
   * @return The size of the hash prefix.
   */
  public static int getHashSize(Object rowKeyFormat) {
    if (rowKeyFormat instanceof RowKeyFormat) {
      return ((RowKeyFormat) rowKeyFormat).getHashSize();
    } else if (rowKeyFormat instanceof RowKeyFormat2) {
      RowKeyFormat2 format2 = (RowKeyFormat2) rowKeyFormat;
      if (null == format2.getSalt()) {
        throw new RuntimeException("This RowKeyFormat2 instance does not specify salt/hashing.");
      } else {
        return format2.getSalt().getHashSize();
      }
    } else {
      throw new RuntimeException("Unsupported Row Key Format");
    }
  }
}
