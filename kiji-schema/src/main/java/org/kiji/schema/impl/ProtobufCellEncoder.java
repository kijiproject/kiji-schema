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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Descriptors.Descriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.layout.CellSpec;

/**
 * Serializes Protocol Buffers for persistence into an HBase cell.
 *
 * <p>
 *   A Protocol Buffer cell encoder is specific to one column in a KijiTable.
 *   Protocol Buffers generated classes know how to encode and decode themselves.
 * <p>
 */
@ApiAudience.Private
public final class ProtobufCellEncoder implements KijiCellEncoder {
  private static final Logger LOG = LoggerFactory.getLogger(ProtobufCellEncoder.class);

  /** Specification of the column encoding. */
  private final CellSpec mCellSpec;

  /** Full-name of the protocol buffer to encode. */
  private final String mProtobufFullName;

  /** Class of the protocol buffer to encode. */
  private final Class<?> mProtoClass;

  // -----------------------------------------------------------------------------------------------

  /**
   * Creates a new <code>KijiCellEncoder</code> instance.
   *
   * @param cellSpec Specification of the cell to encode.
   * @throws IOException on I/O error.
   */
  public ProtobufCellEncoder(final CellSpec cellSpec) throws IOException {
    mCellSpec = Preconditions.checkNotNull(cellSpec);
    Preconditions.checkArgument(cellSpec.getCellSchema().getType() == SchemaType.PROTOBUF);
    mProtobufFullName = cellSpec.getCellSchema().getProtobufFullName();
    Preconditions.checkNotNull(mProtobufFullName);

    final String className = cellSpec.getCellSchema().getProtobufClassName();
    Class<?> protoClass = null;
    try {
      protoClass = Class.forName(className);
      final Descriptor descriptor =
          (Descriptor) protoClass.getMethod("getDescriptor").invoke(protoClass);
      if (!Objects.equal(descriptor.getFullName(), mProtobufFullName)) {
        throw new IOException(String.format(
            "Protocol buffer from class %s has full name %s, "
            + "and does not match expected protocol buffer name %s.",
            className, descriptor.getFullName(), mProtobufFullName));
      }
    } catch (NoSuchMethodException nsme) {
      LOG.error("Unable to find protocol buffer method : {}.getDescriptor()", className);
      protoClass = null;
    } catch (IllegalAccessException iae) {
      LOG.error("Illegal access on protocol buffer method : {}.getDescriptor(): {}",
          className, iae);
      protoClass = null;
    } catch (InvocationTargetException ite) {
      LOG.error("Invocation target exception on protocol buffer method : {}.getDescriptor(): {}",
          className, ite);
      protoClass = null;
    } catch (ClassNotFoundException cnfe) {
      LOG.error("Unable to load protocol buffer class: {}", className);
    }
    mProtoClass = protoClass;
  }

  /** {@inheritDoc} */
  @Override
  public byte[] encode(final DecodedCell<?> cell) throws IOException {
    return encode(cell.getData());
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T> byte[] encode(final T cellValue) throws IOException {
    final AbstractMessage message = (AbstractMessage) cellValue;
    if (mProtoClass != message.getClass()) {
      throw new IOException(String.format(
          "Protocol buffer of class '%s' (message name '%s') "
          + "does not match expected class '%s' (message name '%s'): %s.",
          cellValue.getClass().getName(), message.getDescriptorForType().getFullName(),
          mProtoClass.getName(), mProtobufFullName,
          cellValue));
    }
    return message.toByteArray();
  }

}
