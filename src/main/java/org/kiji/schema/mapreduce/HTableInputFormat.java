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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * An InputFormat for MapReduce jobs over HBase HTables.
 *
 * <p>HBase ships with a <code>TableInputFormat</code>, but it doesn't clean up its
 * HTable/Zookeeper connections correctly, so we've extended it here and fixed the bug.
 * Otherwise, it behaves exactly like the HBase TableInputFormat.</p>
 */
public class HTableInputFormat extends TableInputFormat {
  /** {@inheritDoc} */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    List<InputSplit> splits = super.getSplits(context);

    // TableInputFormat opens an HTable within its setConf() method, since it is
    // required during getSplits().
    HTableInterface openedTable = getHTable();
    assert null != openedTable;

    // After getSplits(), it is no longer necessary, so let's close it.
    openedTable.close();
    setHTable(null);

    return splits;
  }
}
