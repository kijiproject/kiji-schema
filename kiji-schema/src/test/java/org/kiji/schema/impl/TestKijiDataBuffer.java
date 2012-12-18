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

package org.kiji.schema.impl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;

public class TestKijiDataBuffer {
  @Test
  public void testBuffering() {
    final EntityIdFactory eif = EntityIdFactory.create(RowKeyFormat.newBuilder()
        .setEncoding(RowKeyEncoding.RAW)
        .build());
    final EntityId idFoo1 = eif.fromKijiRowKey("foo1");
    final EntityId idFoo2 = eif.fromKijiRowKey("foo2");
    final EntityId idFoo3 = eif.fromKijiRowKey("foo3");
    final EntityId idFoo4 = eif.fromKijiRowKey("foo4");

    Map<EntityId, List<String>> expectedBuffer = new HashMap<EntityId, List<String>>();
    List<String> foo1List = new ArrayList<String>();
    foo1List.add("bar1");
    foo1List.add("bar3");
    List<String> foo2List = new ArrayList<String>();
    foo2List.add("bar2");
    List<String> foo3List = new ArrayList<String>();
    foo3List.add("bar4");
    foo3List.add("bar5");
    foo3List.add("bar6");
    List<String> foo4List = new ArrayList<String>();
    foo4List.add("bar7");
    expectedBuffer.put(idFoo1, foo1List);
    expectedBuffer.put(idFoo2, foo2List);
    expectedBuffer.put(idFoo3, foo3List);
    expectedBuffer.put(idFoo4, foo4List);

    Collection<List<String>> expectedSingles = expectedBuffer.values();
    Set<Map.Entry<EntityId, List<String>>> expectedEntrys = expectedBuffer.entrySet();
    KijiDataBuffer<String> buffer = new KijiDataBuffer<String>();

    buffer.add(idFoo1, "bar1");
    buffer.add(idFoo2, "bar2");
    buffer.add(idFoo1, "bar3");
    buffer.add(idFoo3, "bar4");
    buffer.add(idFoo3, "bar5");
    buffer.add(idFoo3, "bar6");
    buffer.add(idFoo4, "bar7");

    assertEquals(7, buffer.size());
    assertArrayEquals(expectedSingles.toArray(), buffer.getBuffers().toArray());
    assertEquals(expectedEntrys, buffer.getEntries());

    buffer.clear();
    assertEquals(0, buffer.size());
  }
}
