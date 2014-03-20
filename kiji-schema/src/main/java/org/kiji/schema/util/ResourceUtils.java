/**
 * (c) Copyright 2014 WibiData, Inc.
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
package org.kiji.schema.util;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReaderPool;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;

/** Utilities to work with ReferenceCountable resources. */
@ApiAudience.Framework
@ApiStability.Evolving
public final class ResourceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);

  /**
   * Exception representing an exception thrown while handling another exception and the original
   * exception.
   */
  public static final class CompoundException extends Exception {

    private final String mMessage;
    private final Exception mFirstException;
    private final Exception mSecondException;

    /**
     * Initialize a new CompoundException.
     *
     * @param message error message for this exception.
     * @param firstException chronologically first exception.
     * @param secondException exception which occurred while handling the first exception.
     */
    public CompoundException(
        final String message,
        final Exception firstException,
        final Exception secondException
    ) {
      mMessage = String.format("%s: first exception: %s second exception: %s",
          message, firstException.getMessage(), secondException.getMessage());
      mFirstException = firstException;
      mSecondException = secondException;
    }

    /** {@inheritDoc} */
    @Override
    public String getMessage() {
      return mMessage;
    }

    /**
     * Get the first exception from this compound exception.
     *
     * @return the first exception from this compound exception.
     */
    public Exception getFirstException() {
      return mFirstException;
    }

    /**
     * Get the second exception from this compound exception.
     *
     * @return the second exception from this compound exception.
     */
    public Exception getSecondException() {
      return mSecondException;
    }
  }

  /**
   * Perform two actions with a resource. Typically the second action will be cleanup.
   *
   * @param <RESOURCE> Type of the resource.
   * @param <RETURN> Type of the return value of the first action.
   */
  public abstract static class DoAnd<RESOURCE, RETURN> {

    /**
     * Open the resource.
     *
     * @return the resource with which to perform both actions.
     * @throws Exception in case of an error opening the resource.
     */
    protected abstract RESOURCE openResource() throws Exception;

    /**
     * Perform the first action with the resource.
     *
     * @param resource the resource with which to perform the action.
     * @return the result of the action.
     * @throws Exception in case of an error performing the action.
     */
    protected abstract RETURN run(RESOURCE resource) throws Exception;

    /**
     * Perform the second action with the resource. Typically this will be cleanup.
     *
     * @param resource the resource with which to perform the action.
     * @throws Exception in case of an error performing the action.
     */
    protected abstract void after(RESOURCE resource) throws Exception;

    /**
     * Open the resource and perform both actions without losing any exceptions.
     *
     * @return the return value of {@link #run(Object)}.
     * @throws Exception in case of an error opening the resource or performing the actions.
     */
    public final RETURN eval() throws Exception {
      Exception exception = null;
      RESOURCE resource = null;
      try {
        resource = openResource();
        return run(resource);
      } catch (Exception e) {
        exception = e;
        throw e;
      } finally {
        try {
          if (resource != null) {
            after(resource);
          }
        } catch (Exception e) {
          if (exception != null) {
            throw new CompoundException("Exception was throw while cleaning up resources after "
                + "another exception was thrown.", exception, e);
          } else {
            throw e;
          }
        }
      }
    }
  }

  /**
   * Perform an action with a {@link java.io.Closeable} resource and then close the resource.
   *
   * @param <RESOURCE> Type of the resource.
   * @param <RET> Return type of the action.
   */
  public abstract static class DoAndClose<RESOURCE extends Closeable, RET>
      extends DoAnd<RESOURCE, RET> {
    /** {@inheritDoc} */
    @Override
    public final void after(
        final RESOURCE closeable
    ) throws IOException {
      closeable.close();
    }
  }

  /**
   * Perform an action with a {@link org.kiji.schema.util.ReferenceCountable} resource and then
   * release the resource.
   *
   * @param <RESOURCE> Type of the resource.
   * @param <RET> Return type of the action.
   */
  public abstract static class DoAndRelease<RESOURCE extends ReferenceCountable<RESOURCE>, RET>
      extends DoAnd<RESOURCE, RET> {
    /** {@inheritDoc} */
    @Override
    public final void after(
        final RESOURCE releasable
    ) throws IOException {
      releasable.release();
    }
  }

  /**
   * Perform an action with a Kiji instance and then release the instance.
   *
   * @param <RET> Return type of the action.
   */
  public abstract static class WithKiji<RET> extends DoAndRelease<Kiji, RET> {
    private final KijiURI mUri;

    /**
     * Initialize a new WithKiji.
     *
     * @param uri KijiURI of the Kiji instance with which to perform the action.
     */
    public WithKiji(
        final KijiURI uri
    ) {
      mUri = uri;
    }

    /** {@inheritDoc} */
    @Override
    public final Kiji openResource() throws IOException {
      return Kiji.Factory.open(mUri);
    }
  }

  /**
   * Perform an action with a KijiTable and then release the table.
   *
   * @param <RET> Return type of the action.
   */
  public abstract static class WithKijiTable<RET> extends DoAndRelease<KijiTable, RET> {
    /** WithKiji that returns a KijiTable from a KijiURI. */
    private final class GetTable extends WithKiji<KijiTable> {

      /**
       * Initialize a new GetTable.
       *
       * @param tableUri KijiURI of the table to return.
       */
      public GetTable(
          final KijiURI tableUri
      ) {
        super(tableUri);
      }

      /** {@inheritDoc} */
      @Override
      protected KijiTable run(
          final Kiji kiji
      ) throws Exception {
        return kiji.openTable(mTableURI.getTable());
      }
    }

    private final KijiURI mTableURI;

    private final Kiji mKiji;
    private final String mTableName;

    /**
     * Initialize a new WithKijiTable.
     *
     * @param tableURI KijiURI of the table with which to perform an action.
     */
    public WithKijiTable(
        final KijiURI tableURI
    ) {
      mTableURI = tableURI;
      mKiji = null;
      mTableName = null;
    }

    /**
     * Initialize a new WithKijiTable.
     *
     * @param kiji Kiji instance from which to get the table.
     * @param tableName Name of the table.
     */
    public WithKijiTable(
        final Kiji kiji,
        final String tableName
    ) {
      mTableURI = null;
      mKiji = kiji;
      mTableName = tableName;
    }

    /** {@inheritDoc} */
    @Override
    public final KijiTable openResource() throws Exception {
      if (null != mTableURI) {
        return new GetTable(mTableURI).eval();
      } else {
        return mKiji.openTable(mTableName);
      }
    }
  }

  /**
   * Perform an action with a KijiTableWriter and then close the writer.
   *
   * @param <RET> Return value of the action.
   */
  public abstract static class WithKijiTableWriter<RET> extends DoAndClose<KijiTableWriter, RET> {

    // Exactly one of these fields must be non-null.
    private final KijiURI mTableURI;
    private final KijiTable mTable;

    /**
     * Initialize a new WithKijiTableWriter.
     *
     * @param tableURI KijiURI of the table from which to get the writer.
     */
    public WithKijiTableWriter(
        final KijiURI tableURI
    ) {
      mTableURI = tableURI;
      mTable = null;
    }

    /**
     * Initialize a new WithKijiTableWriter.
     *
     * @param table KijiTable from which to get the writer.
     */
    public WithKijiTableWriter(
        final KijiTable table
    ) {
      mTableURI = null;
      mTable = table;
    }

    /** {@inheritDoc} */
    @Override
    public final KijiTableWriter openResource() throws Exception {
      if (null != mTableURI) {
        final WithKijiTable<KijiTableWriter> wkt = new WithKijiTable<KijiTableWriter>(mTableURI) {
          @Override
          public KijiTableWriter run(final KijiTable kijiTable) throws Exception {
            return kijiTable.openTableWriter();
          }
        };
        return wkt.eval();
      } else {
        return mTable.openTableWriter();
      }
    }
  }

  /**
   * Perform an action with a KijiTableReader and then close the reader.
   *
   * @param <RET> type of the return value of the action.
   */
  public abstract static class WithKijiTableReader<RET> extends DoAndClose<KijiTableReader, RET> {

    // Exactly one of these fields must be non-null.
    private final KijiURI mTableURI;
    private final KijiTable mTable;
    private final KijiTableReaderPool mPool;

    /**
     * Initialize a new WithKijiTableReader.
     *
     * @param table KijiTable from which to get the reader.
     */
    public WithKijiTableReader(
        final KijiTable table
    ) {
      mTableURI = null;
      mTable = table;
      mPool = null;
    }

    /**
     * Initialize a new WithKijiTableReader.
     *
     * @param tableURI KijiURI of the table from which to get the reader.
     */
    public WithKijiTableReader(
        final KijiURI tableURI
    ) {
      mTableURI = tableURI;
      mTable = null;
      mPool = null;
    }

    /**
     * Initialize a new WithKijiTableReader.
     *
     * @param pool KijiTableReaderPool from which to get the reader with which to perform an action.
     */
    public WithKijiTableReader(
        final KijiTableReaderPool pool
    ) {
      mTableURI = null;
      mTable = null;
      mPool = pool;
    }

    /** {@inheritDoc} */
    @Override
    public final KijiTableReader openResource() throws Exception {
      if (null != mTableURI) {
        final WithKijiTable<KijiTableReader> wkt = new WithKijiTable<KijiTableReader>(mTableURI) {
          @Override
          public KijiTableReader run(final KijiTable kijiTable) throws Exception {
            return kijiTable.openTableReader();
          }
        };
        return wkt.eval();
      } else if (null != mTable) {
        return mTable.openTableReader();
      } else {
        return mPool.borrowObject();
      }
    }
  }

  /**
   * Closes the specified resource, logging and swallowing I/O errors if needed.
   *
   * @param resource Close this resource.
   */
  public static void closeOrLog(Closeable resource) {
    if (resource == null) {
      return;
    }
    try {
      resource.close();
    } catch (IOException ioe) {
      LOG.warn("I/O error while closing resource '{}':\n{}",
          resource, StringUtils.stringifyException(ioe));
    }
  }

  /**
   * Releases the specified resource, logging and swallowing I/O errors if needed.
   *
   * @param resource Release this resource.
   * @param <T> Type of the resource to release.
   */
  public static <T extends ReferenceCountable<T>> void releaseOrLog(
      ReferenceCountable<T> resource) {
    if (resource == null) {
      return;
    }
    try {
      resource.release();
    } catch (IOException ioe) {
      LOG.warn("I/O error while releasing resource '{}':\n{}",
          resource, StringUtils.stringifyException(ioe));
    }
  }

  /** Utility class cannot be instantiated. */
  private ResourceUtils() { }
}
