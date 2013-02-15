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

package org.kiji.schema;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

public class TestKijiDataRequest {
  @Test
  public void testSerializability() throws IOException, ClassNotFoundException {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("foo", "bar1")
        .add("foo", "bar2")
        .add("foo", "bar3")
        .add("foo", "bar4");
    KijiDataRequest expected = builder.build();

    ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
    new ObjectOutputStream(byteOutput).writeObject(expected);

    byte[] bytes = byteOutput.toByteArray();

    ByteArrayInputStream byteInput = new ByteArrayInputStream(bytes);
    KijiDataRequest actual = (KijiDataRequest) (new ObjectInputStream(byteInput).readObject());

    assertEquals(expected, actual);
  }

  @Test
  public void testColumnRequestEquals() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("foo", "bar");
    KijiDataRequest req0 = builder.build();

    builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("foo", "bar");
    KijiDataRequest req1 = builder.build();
    assertTrue(req0 != req1);
    assertEquals(req0, req0);
    KijiDataRequest.Column foo0 = req0.getColumn("foo", "bar");
    KijiDataRequest.Column foo1 = req1.getColumn("foo", "bar");
    assertEquals(foo0, foo0);
    assertEquals(foo0, foo1);
    assertEquals(foo1, foo0);

    builder = KijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(2).add("foo", "bar");
    builder.newColumnsDef().add("foo", "baz");
    KijiDataRequest req2 = builder.build();
    KijiDataRequest.Column foo2 = req2.getColumn("foo", "bar");
    assertThat(new Object(), is(not((Object) foo2)));
    assertFalse(foo0.equals(foo2));
    assertFalse(foo2.equals(foo0));
    assertThat(foo1, is(not(foo2)));

    KijiDataRequest.Column foo3 = req2.getColumn("foo", "baz");
    assertFalse(foo0.equals(foo3));
    assertThat(foo1, is(not(foo3)));
  }

  @Test
  public void testDataRequestEquals() {
    KijiDataRequestBuilder builder0 = KijiDataRequest.builder()
        .withTimeRange(3L, 4L);
    builder0.newColumnsDef().withMaxVersions(2).addFamily("foo");
    builder0.newColumnsDef().withMaxVersions(5).add("bar", "baz");
    KijiDataRequest request0 = builder0.build();

    KijiDataRequestBuilder builder1 = KijiDataRequest.builder()
        .withTimeRange(3L, 4L);
    builder1.newColumnsDef().withMaxVersions(2).addFamily("foo");
    builder1.newColumnsDef().withMaxVersions(5).add("bar", "baz");
    KijiDataRequest request1 = builder1.build();

    KijiDataRequestBuilder builder2 = KijiDataRequest.builder()
        .withTimeRange(3L, 4L);
    builder2.newColumnsDef().withMaxVersions(2).addFamily("foo");
    builder2.newColumnsDef().withMaxVersions(5).add("car", "bot");
    KijiDataRequest request2 = builder2.build();

    KijiDataRequestBuilder builder3 = KijiDataRequest.builder()
        .withTimeRange(3L, 4L);
    builder3.newColumnsDef().withMaxVersions(2).addFamily("foo");
    builder3.newColumnsDef().withMaxVersions(3).add("car", "bot");
    KijiDataRequest request3 = builder3.build();

    assertEquals(request0, request1);
    assertThat(new Object(), is(not((Object) request0)));
    assertThat(request0, is(not(request2)));
    assertThat(request2, is(not(request3)));
  }

  @Test
  public void testMerge() {
    KijiDataRequestBuilder builder1 = KijiDataRequest.builder().withTimeRange(3, 4);
    builder1.newColumnsDef().withMaxVersions(2).add("foo", "bar");
    KijiDataRequest first = builder1.build();

    KijiDataRequestBuilder builder2 = KijiDataRequest.builder().withTimeRange(2, 4);
    builder2.newColumnsDef().add("baz", "bot");
    builder2.newColumnsDef().withMaxVersions(6).add("foo", "bar");
    KijiDataRequest second = builder2.build();

    KijiDataRequest merged = first.merge(second);
    assertTrue("merge() cannot mutate the object in place", first != merged);

    KijiDataRequest.Column fooBarColumnRequest = merged.getColumn("foo", "bar");
    assertNotNull("Missing column foo:bar from merged request", fooBarColumnRequest);
    assertEquals("Max versions was not increased", 6, fooBarColumnRequest.getMaxVersions());
    assertEquals("Time range was not extended", 2L, merged.getMinTimestamp());
    assertEquals(4L, merged.getMaxTimestamp());

    KijiDataRequest.Column bazBotColumnRequest = merged.getColumn("baz", "bot");
    assertNotNull("Missing column from merged-in request", bazBotColumnRequest);

    KijiDataRequest symmetricMerged = second.merge(first);
    assertEquals("Merge must be symmetric", merged, symmetricMerged);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidColumnSpec() {
    // The user really wants 'builder.columns().add("family", "qualifier")'.
    // This will throw an exception.
    KijiDataRequest.builder().newColumnsDef().addFamily("family:qualifier");
  }

  @Test
  public void testPageSize() {
    final KijiDataRequestBuilder builder1 = KijiDataRequest.builder();
    builder1.newColumnsDef().withPageSize(1).add("foo", "bar");
    final KijiDataRequest first = builder1.build();

    final KijiDataRequestBuilder builder2 = KijiDataRequest.builder();
    builder2.newColumnsDef().add("foo", "bar");
    final KijiDataRequest second = builder2.build();

    assertThat(first, is(not(second)));
    assertFalse(first.equals(second));
    assertFalse(second.equals(first));
  }

  @Test
  public void testPageSizeMerge() {
    // Page size should merge to the smallest value.

    final KijiDataRequestBuilder builder1 = KijiDataRequest.builder();
    builder1.newColumnsDef().withPageSize(1).add("foo", "bar");
    final KijiDataRequest first = builder1.build();

    final KijiDataRequestBuilder builder2 = KijiDataRequest.builder();
    builder2.newColumnsDef().withPageSize(3).add("foo", "bar");
    final KijiDataRequest second = builder2.build();

    assertEquals("Unexpected page size for 'first'",
        1, first.getColumn("foo", "bar").getPageSize());
    assertEquals("Unexpected page size for 'second'",
        3, second.getColumn("foo", "bar").getPageSize());

    final KijiDataRequest merge1 = first.merge(second);
    final KijiDataRequest merge2 = second.merge(first);
    assertEquals("Merged results should be symmetric", merge1, merge2);
    assertEquals("Unexpected merged page size",
        1, merge1.getColumn("foo", "bar").getPageSize());
  }

  @Test
  public void testPageSizeMergeWithZero() {
    // ... unless the smallest value is zero, in which case we go with the
    // non-zero value.

    final KijiDataRequestBuilder builder1 = KijiDataRequest.builder();
    builder1.newColumnsDef().withPageSize(4).add("foo", "bar");
    final KijiDataRequest first = builder1.build();

    final KijiDataRequestBuilder builder2 = KijiDataRequest.builder();
    builder2.newColumnsDef().add("foo", "bar");
    final KijiDataRequest second = builder2.build();

    assertEquals("Unexpected page size for 'first'",
        4, first.getColumn("foo", "bar").getPageSize());
    assertEquals("Unexpected page size for 'second'",
        0, second.getColumn("foo", "bar").getPageSize());

    final KijiDataRequest merge1 = first.merge(second);
    final KijiDataRequest merge2 = second.merge(first);
    assertEquals("Merged results should be symmetric", merge1, merge2);
    assertEquals("Unexpected merged page size",
        4, merge1.getColumn("foo", "bar").getPageSize());
  }
}
