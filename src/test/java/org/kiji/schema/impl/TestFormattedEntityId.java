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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.kiji.schema.avro.*;

/** Tests for FormattedEntityId. */
public class TestFormattedEntityId {
  @Test
  public void testFormattedEntityId() {
    // Fake list of storage encodings including a string, number, hash of string
    // component and hash of number component.
    ArrayList<StorageEncoding> storageEncodings = new ArrayList<StorageEncoding>();
    storageEncodings.add(StorageEncoding.newBuilder().setComponentName("HS")
        .setTransform(KeyTransform.HASH).setHashSize(4).setHashType(HashType.MD5)
        .setTarget("ASTRING").build());
    storageEncodings.add(StorageEncoding.newBuilder().setComponentName("ASTRING")
        .setTransform(KeyTransform.IDENTITY).build());
    storageEncodings.add(StorageEncoding.newBuilder().setComponentName("HN")
        .setTransform(KeyTransform.HASH).setHashSize(4).setHashType(HashType.MD5)
        .setTarget("ANUMBER").build());
    storageEncodings.add(StorageEncoding.newBuilder().setComponentName("ANUMBER")
        .setTransform(KeyTransform.IDENTITY).build());
    storageEncodings.add(StorageEncoding.newBuilder().setComponentName("ALONG")
        .setTransform(KeyTransform.IDENTITY).build());

    // Create a fake Component Key Spec
    HashMap<String, ComponentType> compMap = new HashMap<String, ComponentType>();
    compMap.put("ASTRING", ComponentType.STRING);
    compMap.put("ANUMBER", ComponentType.INTEGER);
    compMap.put("ALONG", ComponentType.LONG);

    // Create a fake row key format to test with
    RowKeyFormat format = RowKeyFormat.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setEncodedKeySpec(storageEncodings)
        .setKeySpec(compMap)
        .build();

    HashMap<String, Object> inputRowKey = new HashMap<String, Object>();
    inputRowKey.put("ASTRING", new String("one"));
    inputRowKey.put("ANUMBER", new Integer(1));
    inputRowKey.put("ALONG", new Long(7L));

    FormattedEntityId formattedEntityId = FormattedEntityId.fromKijiRowKey(inputRowKey, format);
    byte[] hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey) {
      System.out.format("%x ", b);
    }

    FormattedEntityId testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    Map actuals = testEntityId.getKijiRowKey();
    assertEquals(compMap.keySet(), actuals.keySet());
    assertEquals(new String("one"), actuals.get("ASTRING"));
    assertEquals(new Integer(1), actuals.get("ANUMBER"));
    assertEquals(new Long(7L), actuals.get("ALONG"));

    // Now try it with a null component
    HashMap<String, Object> partNullRowKey = new HashMap<String, Object>();
    partNullRowKey.put("ASTRING", new String("one"));
    partNullRowKey.put("ANUMBER", new Integer(1));
    partNullRowKey.put("ALONG", null);

    formattedEntityId = FormattedEntityId.fromKijiRowKey(partNullRowKey, format);
    hbaseRowKey = formattedEntityId.getHBaseRowKey();
    assertNotNull(hbaseRowKey);
    System.out.println("Hbase Key is: ");
    for (byte b: hbaseRowKey) {
      System.out.format("%x ", b);
    }

    testEntityId = FormattedEntityId.fromHBaseRowKey(hbaseRowKey, format);
    actuals = testEntityId.getKijiRowKey();
    assertEquals(compMap.keySet(), actuals.keySet());
    assertEquals(new String("one"), actuals.get("ASTRING"));
    assertEquals(new Integer(1), actuals.get("ANUMBER"));
    assertEquals(null, actuals.get("ALONG"));
  }

}
