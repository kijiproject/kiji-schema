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

package org.kiji.schema.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import org.kiji.annotations.ApiAudience;

/**
 * KijiOutput is a class used to wrap {@link KijiMutation}s. This class is necessary to
 * use when writing to Kiji from within a map-reduce job because Hadoop's job runners only
 * allow writing the exact specified key/valye types, not subclasses.
 *
 * KijiOutput should be used as follows:
 * <code>
 *   KijiPut put = ...
 *   context.write(NullWritable.get(), new KijiOutput(put));
 * </code>
 */
@ApiAudience.Public
public final class KijiOutput implements Writable {
  private KijiMutation mOperation;

  /** Empty constructor for Writable serialization. */
  public KijiOutput() { }

  /**
   * Builds a KijiOutput from an existing KijiMutation.
   *
   * @param operation The object to use to construct this KijiOutput.
   */
  public KijiOutput(KijiMutation operation) {
    mOperation = operation;
  }

  /**
   * @return The wrapped KijiMutation.
   */
  public KijiMutation getOperation() {
    return mOperation;
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    // Serialize the wrapped operation type.
    out.writeUTF(mOperation.getClass().getName());

    // Serialize the wrapped operation.
    mOperation.write(out);
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    // Create a new empty instance of KijiMutation if an instance doesn't already exist.
    try {
      mOperation = (KijiMutation) Class.forName(in.readUTF()).newInstance();
    } catch (ClassNotFoundException ex) {
      throw new IOException("Unrecognized KijiMutation found!", ex);
    } catch (InstantiationException ex) {
      throw new IOException(ex);
    } catch (IllegalAccessException ex) {
      throw new IOException(ex);
    }

    // Populate the wrapped operation.
    mOperation.readFields(in);
  }
}
