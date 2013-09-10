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
import java.lang.reflect.Method;

import com.google.common.base.Preconditions;
import com.google.protobuf.AbstractMessageLite;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.layout.CellSpec;

/**
 * Deserializes an HBase cell encoded as a Protocol Buffer.
 *
 * @param <T> The type of the decoded cell data. Should be a Protocol Buffer generated class.
 */
@ApiAudience.Private
public class ProtobufCellDecoder<T> implements KijiCellDecoder<T> {

  /** Specification of the cell encoding. */
  private final CellSpec mCellSpec;

  /** Class of the protocol buffer to decode. */
  private final Class<? extends AbstractMessageLite> mProtoClass;

  /** 'parseFrom(byte[] bytes)' static method of the protocol buffer class to decode. */
  private final Method mParseFromMethod;

  // -----------------------------------------------------------------------------------------------

  /**
   * Initializes an abstract KijiAvroCellDecoder.
   *
   * @param cellSpec Specification of the cell encoding.
   * @throws IOException on I/O error.
   */
  public ProtobufCellDecoder(CellSpec cellSpec) throws IOException {
    mCellSpec = Preconditions.checkNotNull(cellSpec);
    Preconditions.checkArgument(
        mCellSpec.getCellSchema().getType() == SchemaType.PROTOBUF);
    final String className = mCellSpec.getCellSchema().getProtobufClassName();
    try {
      mProtoClass = (Class<? extends AbstractMessageLite>) Class.forName(className);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    }

    try {
      mParseFromMethod = mProtoClass.getMethod("parseFrom", byte[].class);
    } catch (NoSuchMethodException nsme) {
      throw new IOException(nsme);
    }
  }

  /** {@inheritDoc} */
  @Override
  public DecodedCell<T> decodeCell(byte[] encodedBytes) throws IOException {
    try {
      return new DecodedCell<T>(null, (T) mParseFromMethod.invoke(mProtoClass, encodedBytes));
    } catch (InvocationTargetException ite) {
      throw new IOException(ite);
    } catch (IllegalAccessException iae) {
      throw new IOException(iae);
    }
  }

  /** {@inheritDoc} */
  @Override
  public T decodeValue(byte[] bytes) throws IOException {
    return decodeCell(bytes).getData();
  }
}
