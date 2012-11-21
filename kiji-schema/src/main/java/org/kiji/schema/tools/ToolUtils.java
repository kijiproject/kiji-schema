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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.util.ByteArrayFormatter;

/**
 * Utility class providing static methods used by command-line tools.
 */
@ApiAudience.Framework
public final class ToolUtils {
  /** Disable this constructor. */
  private ToolUtils() {}

  /**
   * Create an EntityId from the --entity-id and --entity-hash arguments to a kiji tool.
   * It is an error for both (or neither) of these to be specified.
   *
   * It is also an error to specify entityHash for a table where hashRowKeys is false.
   * "Unspecified" means null or the empty string ("").
   * "Specified" means a non-null, non-empty string.
   *
   * @param entityId The unhashed entity name which should be turned into an EntityId.
   * @param entityHash The hashed entity name which should be used directly.
   * @param format Format used to encode row keys.
   * @return the entity id as specified in the flags.
   * @throws RuntimeException if arguments are invalid.
   * @throws IOException on I/O error.
   */
  public static EntityId createEntityIdFromUserInputs(
      String entityId,
      String entityHash,
      RowKeyFormat format)
      throws IOException {

    final boolean hashSpecified = (null != entityHash) && !entityHash.isEmpty();
    final boolean idSpecified = (null != entityId) && !entityId.isEmpty();
    Preconditions.checkArgument(hashSpecified ^ idSpecified,
        "Must specify exactly one of --entity-id or --entity-hash.");

    final EntityIdFactory eidFactory = EntityIdFactory.getFactory(format);
    if (hashSpecified) {
      final byte[] hbaseRowKey = ByteArrayFormatter.parseHex(entityHash);
      return eidFactory.getEntityIdFromHBaseRowKey(hbaseRowKey);
    } else {
      return eidFactory.getEntityId(entityId);
    }
  }

  /**
   * Parses a flag describing a row key.
   *
   * Row keys can be specified as:
   * <ul>
   *   <li> a sequence of bytes, in hexadecimal representation, e.g. "hex:00dead00beef"
   *   <li> a UTF8 encoded text row key, e.g. "row-key" or "utf8:row-key".
   * </ul>
   *
   * @param flag Row key description.
   * @return the byte array as specified in the flag.
   * @throws IOException if unable to decode the flag.
   */
  public static byte[] parseRowKeyFlag(String flag) throws IOException {
    if (flag.startsWith("hex:")) {
      return ByteArrayFormatter.parseHex(flag.substring(4));
    } else if (flag.startsWith("utf8:")) {
      return Bytes.toBytes(flag.substring(5));
    } else {
      return Bytes.toBytes(flag);
    }
  }
}
