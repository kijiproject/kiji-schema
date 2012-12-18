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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.testutil.FooTableIntegrationTest;

public class IntegrationTestKijiTableInputFormat
    extends FooTableIntegrationTest {
  public static class TestMapper
      extends Mapper<EntityId, KijiRowData, Text, Text> {

    @Override
    public void map(EntityId entityId, KijiRowData row, Context context)
        throws IOException, InterruptedException {
      final String name = row.getStringValue("info", "name").toString();
      final String email = row.getStringValue("info", "email").toString();

      // Build email domain regex.
      final Pattern emailRegex = Pattern.compile(".+@(.+)");
      final Matcher emailMatcher = emailRegex.matcher(email);

      // Extract domain from email.
      assertTrue(emailMatcher.find());
      final String emailDomain = emailMatcher.group(1);

      context.write(new Text(emailDomain), new Text(name));
    }
  }

  public static class TestReducer
      extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // Combine all the names.
      final Iterator<Text> iter = values.iterator();
      final StringBuilder names = new StringBuilder(iter.next().toString());
      while (iter.hasNext()) {
        final Text name = iter.next();

        names.append(",");
        names.append(name.toString());
      }

      // Write names to context.
      final Text output = new Text(names.toString().trim());
      context.write(key, output);
    }
  }

  public Job setupJob() throws Exception {
    final Job job = new Job(createConfiguration());

    // Get settings for test.
    final String instance = getInstanceName();
    final String table = getFooTable().getName();
    final KijiDataRequest request = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "name"))
        .addColumn(new KijiDataRequest.Column("info", "email"));

    job.setJarByClass(IntegrationTestKijiTableInputFormat.class);

    // Setup the InputFormat.
    KijiTableInputFormat.setOptions(job, instance, table, request);
    job.setInputFormatClass(KijiTableInputFormat.class);

    return job;
  }

  /**
   * Test KijiTableInputFormat in a map-only job.
   */
  @Test
  public void testMapJob() throws Exception {
    // Create a test job.
    final Path outputFile = new Path("/foo/part-r-00000");
    final Job job = setupJob();
    job.setJobName("testMapJob");

    // Setup the OutputFormat.
    TextOutputFormat.setOutputPath(job, outputFile.getParent());
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    // Set the mapper class.
    job.setMapperClass(TestMapper.class);

    // Run the job.
    assertTrue("Hadoop job failed", job.waitForCompletion(true));

    // Check to make sure output exists.
    final FileSystem fs = FileSystem.get(job.getConfiguration());
    assertTrue(fs.exists(outputFile.getParent()));

    // Verify that the output matches what's expected.
    final FSDataInputStream in = fs.open(outputFile);
    final Set<String> actual = Sets.newHashSet(IOUtils.toString(in).trim().split("\n"));
    final Set<String> expected = Sets.newHashSet(
        "wibidata.com\tAaron Kimball",
        "gmail.com\tJohn Doe",
        "wibidata.com\tChristophe Bisciglia",
        "wibidata.com\tKiyan Ahmadizadeh",
        "gmail.com\tJane Doe",
        "wibidata.com\tGarrett Wu");
    assertEquals("Result of job wasn't what was expected", expected, actual);

    // Clean up.
    fs.delete(outputFile.getParent(), true);

    IOUtils.closeQuietly(in);
    // NOTE: fs should get closed here, but doesn't because of a bug with FileSystem that
    // causes it to close other thread's filesystem objects. For more information
    // see: https://issues.apache.org/jira/browse/HADOOP-7973
  }

  /**
   * Test KijiTableInputFormat in a map-reduce job.
   */
  @Test
  public void testMapReduceJob() throws Exception {
    // Create a test job.
    final Path outputFile = new Path("/foo/part-r-00000");
    final Job job = setupJob();
    job.setJobName("testMapReduceJob");

    // Setup the OutputFormat
    TextOutputFormat.setOutputPath(job, outputFile.getParent());
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    // Set the mapper and reducer.
    job.setMapperClass(TestMapper.class);
    job.setReducerClass(TestReducer.class);

    // Run the job.
    assertTrue("Hadoop job failed", job.waitForCompletion(true));

    // Check to make sure output exists.
    final FileSystem fs = FileSystem.get(job.getConfiguration());
    assertTrue(fs.exists(outputFile.getParent()));

    // Verify that the output matches what's expected.
    final FSDataInputStream in = fs.open(outputFile);
    final Set<String> output = Sets.newHashSet(IOUtils.toString(in).trim().split("\n"));
    final ImmutableMap.Builder<String, Set<String>> builder = ImmutableMap.builder();
    for (String line : output) {
      final String[] keyValue = line.split("\t");
      final String emailDomain = keyValue[0];
      final Set<String> names = Sets.newHashSet(keyValue[1].split(","));

      builder.put(emailDomain, names);
    }
    final Map<String, Set<String>> actual = builder.build();
    final Map<String, Set<String>> expected = ImmutableMap.<String, Set<String>>builder()
        .put("wibidata.com", Sets.newHashSet(
              "Aaron Kimball",
              "Christophe Bisciglia",
              "Kiyan Ahmadizadeh",
              "Garrett Wu"))
        .put("gmail.com", Sets.newHashSet(
              "John Doe",
              "Jane Doe"))
        .build();
    assertEquals("Result of job wasn't what was expected", expected, actual);

    // Clean up.
    fs.delete(outputFile.getParent(), true);

    IOUtils.closeQuietly(in);
    // NOTE: fs should get closed here, but doesn't because of a bug with FileSystem that
    // causes it to close other thread's filesystem objects. For more information
    // see: https://issues.apache.org/jira/browse/HADOOP-7973
  }
}
