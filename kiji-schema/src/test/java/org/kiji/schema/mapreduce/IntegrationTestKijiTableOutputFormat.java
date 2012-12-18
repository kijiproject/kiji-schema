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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.impl.RawEntityId;
import org.kiji.schema.testutil.FooTableIntegrationTest;


public class IntegrationTestKijiTableOutputFormat
    extends FooTableIntegrationTest {

  public static class TestMapper
      extends Mapper<EntityId, KijiRowData, NullWritable, KijiOutput> {

    @Override
    public void map(EntityId entityId, KijiRowData row, Context context)
        throws IOException, InterruptedException {
      final String name = row.getStringValue("info", "name").toString();
      final String email = row.getStringValue("info", "email").toString();
      final EntityId rowKey = row.getEntityId();
      final ContextKijiTableWriter writer = new ContextKijiTableWriter(context);

      // Build email domain regex.
      final Pattern emailRegex = Pattern.compile("(.+)@.+");
      final Matcher emailMatcher = emailRegex.matcher(email);

      // Extract domain from email.
      assertTrue(emailMatcher.find());
      final String account = emailMatcher.group(1);

      writer.put(rowKey, "info", "email", account);
      if (account.length() % 2 == 0) {
        writer.setCounter(rowKey, "other", "counter", account.length());
      } else {
        writer.increment(rowKey, "other", "counter", account.length());
      }
      IOUtils.closeQuietly(writer);
    }
  }

  // Mapper to test KijiDeletes
  public static class TestDeleteMapper
      extends Mapper<EntityId, KijiRowData, NullWritable, KijiOutput> {

    @Override
    public void map(EntityId entityId, KijiRowData row, Context context)
        throws IOException, InterruptedException {
      final EntityId rowKey = row.getEntityId();
      final ContextKijiTableWriter writer = new ContextKijiTableWriter(context);

      writer.deleteCell(rowKey, "info", "b");
      IOUtils.closeQuietly(writer);
    }
  }

  public Job setupJob(KijiDataRequest request) throws Exception {
    final Configuration conf = createConfiguration();
    final Job job = new Job(conf);

    job.setJarByClass(IntegrationTestKijiTableOutputFormat.class);

    final String instance = getInstanceName();
    final String table = getFooTable().getName();

    // Setup the InputFormat.
    KijiTableInputFormat.setOptions(job, instance, table, request);
    job.setInputFormatClass(KijiTableInputFormat.class);

    // Setup the OutputFormat.
    KijiTableOutputFormat.setOptions(job, instance, table);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(KijiOutput.class);
    job.setOutputFormatClass(KijiTableOutputFormat.class);

    return job;
  }

  @Test
  public void testOutputFormat() throws Exception {
    // Create a test job.
    KijiDataRequest request = new KijiDataRequest()
            .addColumn(new KijiDataRequest.Column("info", "name"))
            .addColumn(new KijiDataRequest.Column("info", "email"));
    final Job job = setupJob(request);
    job.setJobName("testOutputFormat");

    // Set the mapper class.
    job.setMapperClass(TestMapper.class);

    // Run the job.
    assertTrue("Hadoop job failed", job.waitForCompletion(true));

    // Read the output from the test mapper.
    final KijiTableReader reader = getFooTable().openTableReader();
    request = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "email"))
        .addColumn(new KijiDataRequest.Column("other", "counter"));

    final ImmutableSet.Builder<String> accountsBuilder = ImmutableSet.builder();
    final ImmutableSet.Builder<Long> countersBuilder = ImmutableSet.builder();
    for (KijiRowData row : reader.getScanner(request, null, null)) {
      final String account = row.getStringValue("info", "email").toString();
      final long counter = row.getCounter("other", "counter").getValue();

      accountsBuilder.add(account);
      countersBuilder.add(counter);
    }
    IOUtils.closeQuietly(reader);

    // Make sure the correct accounts have been output.
    final Set<String> actualAccounts = accountsBuilder.build();
    final Set<String> expectedAccounts =
        ImmutableSet.of("aaron", "john.doe", "christophe", "kiyan", "jane.doe", "gwu");
    assertEquals(expectedAccounts, actualAccounts);

    // Make sure the correct counter values have been output.
    final Set<Long> actualCounters = countersBuilder.build();
    final Set<Long> expectedCounters = ImmutableSet.of(5L, 8L, 10L, 3L);
    assertEquals(expectedCounters, actualCounters);
  }

  @Test
  public void testKijiDelete() throws Exception {
    final KijiTableWriter writer = getFooTable().openTableWriter();
    EntityId entityId;

    // Create some data to keep and some to delete.
    entityId = RawEntityId.fromKijiRowKey(Bytes.toBytes("testDelete"));
    writer.put(entityId, "info", "b", "randomData");
    entityId = RawEntityId.fromKijiRowKey(Bytes.toBytes("testKeep"));
    writer.put(entityId, "info", "b", "randomDeleteData");
    IOUtils.closeQuietly(writer);

    final KijiDataRequest request = new KijiDataRequest()
            .addColumn(new KijiDataRequest.Column("info", "b"));
    final Job job = setupJob(request);
    job.setJobName("TestKijiDelete");

    // Set the mapper class.
    job.setMapperClass(TestDeleteMapper.class);

    // Run the job.
    assertTrue("Hadoop job failed", job.waitForCompletion(true));

    final KijiTableReader reader = getFooTable().openTableReader();
    assertFalse(reader.getScanner(request, null, null).iterator().hasNext());
    IOUtils.closeQuietly(reader);
  }
}
