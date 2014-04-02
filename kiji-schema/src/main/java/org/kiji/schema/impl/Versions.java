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

package org.kiji.schema.impl;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.util.ProtocolVersion;

/**
 * System and layout versions known and used by Kiji.
 */
@ApiAudience.Private
public final class Versions {

  // -----------------------------------------------------------------------------------------------
  // System versions
  // -----------------------------------------------------------------------------------------------

  /**
   * System version that introduces table layouts in ZooKeeper and brings layout validation.
   */
  public static final ProtocolVersion SYSTEM_2_0 = ProtocolVersion.parse("system-2.0");

  /** Initial system version, as released in KijiSchema 1.0. */
  public static final ProtocolVersion SYSTEM_1_0 = ProtocolVersion.parse("system-1.0");

  /**
   * Deprecated version from old 1.0.0-rc releases that used 'kiji-1.0' as the instance format
   * version. This version is now equivalent to 'system-1.0'.
   */
  public static final ProtocolVersion SYSTEM_KIJI_1_0_DEPRECATED =
      ProtocolVersion.parse("kiji-1.0");

  // -----------------------------------------------------------------------------------------------
  // Aliases and semantic names

  /** Maximum system version supported by this Kiji client. */
  public static final ProtocolVersion MAX_SYSTEM_VERSION = SYSTEM_2_0;

  /** Minimum system version required for table layout validation. */
  public static final ProtocolVersion MIN_SYS_VER_FOR_LAYOUT_VALIDATION = SYSTEM_2_0;

  /** Minimum system version supported by this Kiji client. */
  public static final ProtocolVersion MIN_SYSTEM_VERSION = SYSTEM_1_0;

  // -----------------------------------------------------------------------------------------------
  // Table layout versions
  // -----------------------------------------------------------------------------------------------

  public static final ProtocolVersion LAYOUT_1_5_0 =
      ProtocolVersion.parse("layout-1.5.0");

  public static final ProtocolVersion LAYOUT_1_4_0 =
      ProtocolVersion.parse("layout-1.4.0");

  public static final ProtocolVersion LAYOUT_1_3_0 =
      ProtocolVersion.parse("layout-1.3.0");

  public static final ProtocolVersion LAYOUT_1_2_0 =
      ProtocolVersion.parse("layout-1.2.0");

  public static final ProtocolVersion LAYOUT_1_1_0 =
      ProtocolVersion.parse("layout-1.1.0");

  public static final ProtocolVersion LAYOUT_1_0_0 =
      ProtocolVersion.parse("layout-1.0.0");

  /**
   * Deprecated layout version from old 1.0.0-rc releases.
   * This is now equivalent to 'layout-1.0.0'.
   */
  public static final ProtocolVersion LAYOUT_KIJI_1_0_0_DEPRECATED =
      ProtocolVersion.parse("kiji-1.0.0");

  // -----------------------------------------------------------------------------------------------
  // Aliases and semantic names

  /** Maximum layout version recognized by this client. */
  public static final ProtocolVersion MAX_LAYOUT_VERSION = LAYOUT_1_5_0;

  /** First layout version where table layout validation may be enabled. */
  public static final ProtocolVersion LAYOUT_VALIDATION_VERSION = LAYOUT_1_3_0;

  /** Layout version that introduces ray bytes cell encoding. */
  public static final ProtocolVersion RAW_BYTES_CELL_ENCODING_VERSION = LAYOUT_1_3_0;

  /** Layout version that introduces protocol buffer cell encoding. */
  public static final ProtocolVersion PROTOBUF_CELL_ENCODING_VERSION = LAYOUT_1_4_0;

  /**
   * Layout version that allows for configuration of column name translation.
   */
  public static final ProtocolVersion CONFIGURE_COLUMN_NAME_TRANSLATION_VERSION = LAYOUT_1_5_0;

  /**
   * Version of the layout that introduces:
   * <ul>
   *   <li> {@link org.kiji.schema.avro.BloomType}; </li>
   *   <li> max_filesize; </li>
   *   <li> memstore_flushsize; </li>
   *   <li> block_size. </li>
   * </ul>
   */
  public static final ProtocolVersion BLOCK_SIZE_LAYOUT_VERSION = LAYOUT_1_2_0;

  /** Version of the layout that introduces {@link RowKeyFormat2}. */
  public static final ProtocolVersion RKF2_LAYOUT_VERSION = LAYOUT_1_1_0;

  /** Minimum layout version recognized by this client. */
  public static final ProtocolVersion MIN_LAYOUT_VERSION = LAYOUT_1_0_0;


  // -----------------------------------------------------------------------------------------------
  // Security versions
  // -----------------------------------------------------------------------------------------------
  public static final ProtocolVersion SECURITY_0_0 = ProtocolVersion.parse("security-0.0");
  public static final ProtocolVersion SECURITY_0_1 = ProtocolVersion.parse("security-0.1");

  // -----------------------------------------------------------------------------------------------
  // Aliases and semantic names

  /** The protocol version representing uninstalled Security. */
  public static final ProtocolVersion UNINSTALLED_SECURITY_VERSION = SECURITY_0_0;

  /** Minimum version where security exists.  At this version it is instance-level R/W/G only. */
  public static final ProtocolVersion MIN_SECURITY_VERSION = SECURITY_0_1;

  // -----------------------------------------------------------------------------------------------

  /** Utility class cannot be instantiated. */
  private Versions() {
  }
}
