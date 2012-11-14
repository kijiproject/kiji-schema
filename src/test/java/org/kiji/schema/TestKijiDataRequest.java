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
    KijiDataRequest expected = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("foo", "bar1"))
        .addColumn(new KijiDataRequest.Column("foo", "bar2"))
        .addColumn(new KijiDataRequest.Column("foo", "bar3"))
        .addColumn(new KijiDataRequest.Column("foo", "bar4"));

    ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
    new ObjectOutputStream(byteOutput).writeObject(expected);

    byte[] bytes = byteOutput.toByteArray();

    ByteArrayInputStream byteInput = new ByteArrayInputStream(bytes);
    KijiDataRequest actual = (KijiDataRequest) (new ObjectInputStream(byteInput).readObject());

    assertEquals(expected, actual);
  }

  @Test
  public void testColumnRequestEquals() {
    KijiDataRequest.Column foo0 = new KijiDataRequest.Column("foo", "bar");
    KijiDataRequest.Column foo1 = new KijiDataRequest.Column("foo", "bar");
    KijiDataRequest.Column foo2 = new KijiDataRequest.Column("foo", "bar").withMaxVersions(2);
    KijiDataRequest.Column foo3 = new KijiDataRequest.Column("foo", "baz");
    assertEquals(foo0, foo1);
    assertThat(foo1, is(not(foo2)));
    assertThat(new Object(), is(not((Object) foo2)));
    assertThat(foo1, is(not(foo3)));
  }

  @Test
  public void testDataRequestEquals() {
    KijiDataRequest request0 = new KijiDataRequest();
    request0.addColumn(new KijiDataRequest.Column("foo").withMaxVersions(2));
    request0.addColumn(new KijiDataRequest.Column("bar", "baz").withMaxVersions(5));
    request0.withTimeRange(3L, 4L);
    KijiDataRequest request1 = new KijiDataRequest();
    request1.addColumn(new KijiDataRequest.Column("foo").withMaxVersions(2));
    request1.addColumn(new KijiDataRequest.Column("bar", "baz").withMaxVersions(5));
    request1.withTimeRange(3L, 4L);
    KijiDataRequest request2 = new KijiDataRequest();
    request2.addColumn(new KijiDataRequest.Column("foo").withMaxVersions(2));
    request2.addColumn(new KijiDataRequest.Column("car", "bot").withMaxVersions(5));
    request2.withTimeRange(3L, 4L);
    KijiDataRequest request3 = new KijiDataRequest();
    request3.addColumn(new KijiDataRequest.Column("foo").withMaxVersions(2));
    request3.addColumn(new KijiDataRequest.Column("car", "bot").withMaxVersions(3));
    request3.withTimeRange(3L, 4L);

    assertEquals(request0, request1);
    assertThat(new Object(), is(not((Object) request0)));
    assertThat(request0, is(not(request2)));
    assertThat(request2, is(not(request3)));
  }

  @Test
  public void testMerge() {
    KijiDataRequest first = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("foo", "bar").withMaxVersions(2))
        .withTimeRange(3, 4);

    KijiDataRequest second = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("baz", "bot"))
        .addColumn(new KijiDataRequest.Column("foo", "bar").withMaxVersions(6))
        .withTimeRange(2, 4);

    assertTrue("merge() should have mutated the object in place", first == first.merge(second));

    KijiDataRequest.Column fooBarColumnRequest = first.getColumn("foo", "bar");
    assertNotNull("Missing column foo:bar from merged request", fooBarColumnRequest);
    assertEquals("Max versions was not increased", 6, fooBarColumnRequest.getMaxVersions());
    assertEquals("Time range was not extended", 2L, first.getMinTimestamp());
    assertEquals(4L, first.getMaxTimestamp());

    KijiDataRequest.Column bazBotColumnRequest = first.getColumn("baz", "bot");
    assertNotNull("Missing column from merged-in request", bazBotColumnRequest);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidColumnSpec() {
    // The user really wants 'new KijiDataRequest.Column("family", "qualifier")'.
    // This will throw an exception.
    new KijiDataRequest.Column("family:qualifier");
  }

  @Test
  public void testPageSize() {
    final KijiDataRequest first = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("foo", "bar").withPageSize(1));
    final KijiDataRequest second = new KijiDataRequest(first);
    final KijiDataRequest third = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("foo", "bar"));

    assertEquals(first, second);
    assertThat(first, is(not(third)));
  }
}
