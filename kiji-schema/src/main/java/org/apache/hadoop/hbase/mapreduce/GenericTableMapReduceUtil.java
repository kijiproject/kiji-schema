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

package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Job;

/**
 * Just like TableMapReduceUtil but fixes some missing dependency jars.
 */
public class GenericTableMapReduceUtil extends TableMapReduceUtil {
  /**
   * Configures the job with an HBase scan.
   *
   * @param scan The scan to set in the job configuration.
   * @param job The job to configure.
   * @throws IOException If there is an error.
   */
  public static void initTableScan(Scan scan, Job job) throws IOException {
    job.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(scan));
    addAllDependencyJars(job);
  }

  /**
   * Configures the job with an HBase scan over a table as input.
   */
  public static void initTableInput(String table, Scan scan, Job job) throws IOException {
    job.getConfiguration().set(TableInputFormat.INPUT_TABLE, table);
    initTableScan(scan, job);
  }

  /**
   * This method is just like the package-private version in TableMapReduceUtil,
   * except that it disables block caching by default.
   */
  public static Scan convertStringToScan(String base64) throws IOException {
    Scan scan = TableMapReduceUtil.convertStringToScan(base64);
    scan.setCacheBlocks(false);
    return scan;
  }

  /**
   * This is just like the TableMapReduceUtil.initTableMapperJob but
   * it takes any classes for input and output keys instead of just
   * Writables.  This way we can work with AvroSerialization instead
   * of just WritableSerialization.
   */
  public static void initGenericTableMapperJob(String table, Scan scan,
      Class<? extends TableMapper<?, ?>> mapper,
      Class<?> outputKeyClass,
      Class<?> outputValueClass, Job job) throws IOException {
    if (outputValueClass != null) {
      job.setMapOutputValueClass(outputValueClass);
    }
    if (outputKeyClass != null) {
      job.setMapOutputKeyClass(outputKeyClass);
    }
    job.setMapperClass(mapper);
    job.getConfiguration().set(TableInputFormat.INPUT_TABLE, table);
    job.getConfiguration().set(TableInputFormat.SCAN,
        convertScanToString(scan));
    addAllDependencyJars(job);
  }

  /**
   * Like TableMapReduceUtil.addDependencyJars() but this catches some
   * missing ones like google's guava library.
   */
  public static void addAllDependencyJars(Job job) throws IOException {
    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
        org.apache.hadoop.hbase.mapreduce.TableInputFormat.class,
        // This is used by the KeyValue class, triggered by
        // PutSortReducer.  Without this, BulkImportJob doesn't work.
        com.google.common.primitives.Longs.class);
  }
}
