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

package org.kiji.schema.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

import org.kiji.schema.KConstants;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;

public class TestHBaseKijiURI {
  @Test
  public void testClusterIdentifier() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost:1234").build();
    assertEquals("kiji-hbase", uri.getScheme());
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals(null, uri.getInstance());
    assertEquals(null, uri.getTable());
    assertTrue(uri.getColumns().isEmpty());
  }

  @Test
  public void testKijiInstanceUri() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost:1234/instance").build();
    assertEquals("kiji-hbase", uri.getScheme());
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals(null, uri.getTable());
    assertTrue(uri.getColumns().isEmpty());
  }

  @Test
  public void testSingleHost() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost:1234/instance/table/col").build();
    assertEquals("kiji-hbase", uri.getScheme());
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testSingleHostGroupColumn() {
    final KijiURI uri =
        KijiURI.newBuilder("kiji-hbase://zkhost:1234/instance/table/family:qualifier").build();
    assertEquals("kiji-hbase", uri.getScheme());
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("family:qualifier", uri.getColumns().get(0).getName());
  }

  @Test
  public void testSingleHostDefaultInstance() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost:1234/default/table/col").build();
    assertEquals("kiji-hbase", uri.getScheme());
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1, uri.getZookeeperQuorum().size());
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("default", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testSingleHostDefaultPort() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost/instance/table/col").build();
    assertEquals("kiji-hbase", uri.getScheme());
    assertEquals(1, uri.getZookeeperQuorum().size());
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(KijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testMultipleHosts() {
    final KijiURI uri =
        KijiURI.newBuilder("kiji-hbase://(zkhost1,zkhost2):1234/instance/table/col").build();
    assertEquals("kiji-hbase", uri.getScheme());
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testMultipleHostsDefaultPort() {
    final KijiURI uri =
        KijiURI.newBuilder("kiji-hbase://zkhost1,zkhost2/instance/table/col").build();
    assertEquals("kiji-hbase", uri.getScheme());
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(KijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testMultipleHostsDefaultPortDefaultInstance() {
    final KijiURI uri =
        KijiURI.newBuilder("kiji-hbase://zkhost1,zkhost2/default/table/col").build();
    assertEquals("kiji-hbase", uri.getScheme());
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(KijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("default", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testIllegalScheme() {
    try {
      KijiURI.newBuilder("kiji-foo:///");
      fail("An exception should have been thrown.");
    } catch (KijiURIException kurie) {
      assertTrue(kurie.getMessage().contains("No parser available for"));
    }
  }

  @Test
  public void testNoAuthority() {
    try {
      KijiURI.newBuilder("kiji-hbase:///");
      fail("An exception should have been thrown.");
    } catch (KijiURIException kurie) {
      assertEquals("Invalid Kiji URI: 'kiji-hbase:///' : ZooKeeper ensemble missing.",
          kurie.getMessage());
    }
  }

  @Test
  public void testMultipleHostsNoParen() {
    try {
      KijiURI.newBuilder("kiji-hbase://zkhost1,zkhost2:1234/instance/table/col");
      fail("An exception should have been thrown.");
    } catch (KijiURIException kurie) {
      assertEquals("Invalid Kiji URI: 'kiji-hbase://zkhost1,zkhost2:1234/instance/table/col' : "
              + "Multiple ZooKeeper hosts must be parenthesized.", kurie.getMessage());
    }
  }

  @Test
  public void testMultipleHostsMultiplePorts() {
    try {
      KijiURI.newBuilder("kiji-hbase://zkhost1:1234,zkhost2:2345/instance/table/col");
      fail("An exception should have been thrown.");
    } catch (KijiURIException kurie) {
      assertEquals("Invalid Kiji URI: 'kiji-hbase://zkhost1:1234,zkhost2:2345/instance/table/col'"
              + " : Invalid ZooKeeper ensemble cluster identifier.", kurie.getMessage());
    }
  }

  @Test
  public void testMultipleColumns() {
    final KijiURI uri =
        KijiURI.newBuilder("kiji-hbase://zkhost1,zkhost2/default/table/col1,col2").build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(KijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("default", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals(2, uri.getColumns().size());
  }

  @Test
  public void testExtraPath() {
    try {
      KijiURI.newBuilder("kiji-hbase://(zkhost1,zkhost2):1234/instance/table/col/extra");
      fail("An exception should have been thrown.");
    } catch (KijiURIException kurie) {
      assertEquals("Invalid Kiji URI: 'kiji-hbase://(zkhost1,zkhost2):1234/"
              + "instance/table/col/extra' : Too many path segments.", kurie.getMessage());
    }
  }

  @Test
  public void testURIWithQuery() {
    final KijiURI uri =
        KijiURI.newBuilder("kiji-hbase://(zkhost1,zkhost2):1234/instance/table/col?query").build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testURIWithFragment() {
    final KijiURI uri =
        KijiURI.newBuilder("kiji-hbase://(zkhost1,zkhost2):1234/instance/table/col#frag").build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testPartialURIZookeeper() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost:1234").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals(null, uri.getInstance());
  }

  @Test
  public void testBasicResolution() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost:1234").build();
    final KijiURI resolved = uri.resolve("testinstance");
    assertEquals("testinstance", resolved.getInstance());
  }

  @Test
  public void testResolution() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost:1234/.unset").build();
    final KijiURI resolved = uri.resolve("testinstance");
    assertEquals("testinstance", resolved.getInstance());
  }

  @Test
  public void testResolutionColumn() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost/instance/table").build();
    final KijiURI resolved = uri.resolve("col");
    assertEquals("col", resolved.getColumns().get(0).getName());
  }

  @Test
  public void testInvalidResolution() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost:1234").build();
    try {
      uri.resolve("instance/table/col/extra");
      fail("An exception should have been thrown.");
    } catch (KijiURIException kurie) {
      assertEquals("Invalid Kiji URI: 'kiji-hbase://zkhost:1234/instance/table/col/extra' : "
              + "Too many path segments.", kurie.getMessage());
    }
  }

  @Test
  public void testToString() {
    String uri = "kiji-hbase://(zkhost1,zkhost2):1234/instance/table/col/";
    assertEquals(uri, KijiURI.newBuilder(uri).build().toString());
    uri = "kiji-hbase://zkhost1:1234/instance/table/col/";
    assertEquals(uri, KijiURI.newBuilder(uri).build().toString());
    uri = "kiji-hbase://zkhost:1234/instance/table/col1,col2/";
    assertEquals(uri, KijiURI.newBuilder(uri).build().toString());
    uri = "kiji-hbase://zkhost:1234/.unset/table/col/";
    assertEquals(uri, KijiURI.newBuilder(uri).build().toString());
  }

  @Test
  public void testNormalizedQuorum() {
    KijiURI uri =
        KijiURI.newBuilder("kiji-hbase://(zkhost1,zkhost2):1234/instance/table/col/").build();
    KijiURI reversedQuorumUri =
        KijiURI.newBuilder("kiji-hbase://(zkhost2,zkhost1):1234/instance/table/col/").build();
    assertEquals(uri.toString(), reversedQuorumUri.toString());
    assertEquals(uri.getZookeeperQuorum(), reversedQuorumUri.getZookeeperQuorum());
  }

  @Test
  public void testNormalizedColumns() {
    KijiURI uri =
        KijiURI.newBuilder("kiji-hbase://(zkhost1,zkhost2):1234/instance/table/col/").build();
    KijiURI reversedColumnURI =
        KijiURI.newBuilder("kiji-hbase://(zkhost2,zkhost1):1234/instance/table/col/").build();
    assertEquals(uri.toString(), reversedColumnURI.toString());
    assertEquals(uri.getColumns(), reversedColumnURI.getColumns());
  }

  @Test
  public void testOrderedQuorum() {
    KijiURI uri =
        KijiURI.newBuilder("kiji-hbase://(zkhost1,zkhost2):1234/instance/table/col/").build();
    KijiURI reversedQuorumUri =
        KijiURI.newBuilder("kiji-hbase://(zkhost2,zkhost1):1234/instance/table/col/").build();
    assertFalse(uri.getZookeeperQuorumOrdered()
            .equals(reversedQuorumUri.getZookeeperQuorumOrdered()));
    assertFalse(uri.toOrderedString().equals(reversedQuorumUri.toOrderedString()));
  }

  @Test
  public void testOrderedColumns() {
    KijiURI uri =
        KijiURI.newBuilder("kiji-hbase://(zkhost1,zkhost2):1234/instance/table/col1,col2/").build();
    KijiURI reversedColumnURI =
        KijiURI.newBuilder("kiji-hbase://(zkhost1,zkhost2):1234/instance/table/col2,col1/").build();
    assertFalse(uri.toOrderedString().equals(reversedColumnURI.toOrderedString()));
    assertFalse(uri.getColumnsOrdered().equals(reversedColumnURI.getColumnsOrdered()));
  }

  /**
   * Tests that KijiURI.newBuilder().build() builds a URI for the default Kiji instance URI.
   *
   * The default Kiji instance URI is environment specific. Hence, this cannot test for explicit
   * values of the ZooKeeper quorum of of the ZooKeeper client port.
   */
  @Test
  public void testKijiURIBuilderDefault() {
    KijiURI uri = KijiURI.newBuilder().build();
    assertTrue(!uri.getZookeeperQuorum().isEmpty());  // Test cannot be more specific.
    // Test cannot validate the value of uri.getZookeeperClientPort().
    assertEquals(KConstants.DEFAULT_INSTANCE_NAME, uri.getInstance());
    assertEquals(null, uri.getTable());
    assertTrue(uri.getColumns().isEmpty());
  }

  @Test
  public void testKijiURIBuilderFromInstance() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost:1234/.unset/table").build();
    KijiURI built = KijiURI.newBuilder(uri).build();
    assertEquals(uri, built);
  }

  @Test
  public void testKijiURIBuilderWithInstance() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost:1234/instance1/table").build();
    assertEquals("instance1", uri.getInstance());
    final KijiURI modified =
        KijiURI.newBuilder(uri).withInstanceName("instance2").build();
    assertEquals("instance2", modified.getInstance());
    assertEquals("instance1", uri.getInstance());
  }

  @Test
  public void testSetColumn() {
    KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost/instance/table/").build();
    assertTrue(uri.getColumns().isEmpty());
    uri =
        KijiURI.newBuilder(uri).withColumnNames(Arrays.asList("testcol1", "testcol2"))
            .build();
    assertEquals(2, uri.getColumns().size());
  }

  @Test
  public void testSetZookeeperQuorum() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost/instance/table/col").build();
    final KijiURI modified = KijiURI.newBuilder(uri)
        .withZookeeperQuorum(new String[] {"zkhost1", "zkhost2"}).build();
    assertEquals(2, modified.getZookeeperQuorum().size());
    assertEquals("zkhost1", modified.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", modified.getZookeeperQuorum().get(1));
  }

  @Test
  public void testTrailingUnset() {
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost/.unset/table/.unset").build();
    KijiURI result = KijiURI.newBuilder(uri).withTableName(".unset").build();
    assertEquals("kiji-hbase://zkhost:2181/", result.toString());
  }

  @Test
  public void testEscapedMapColumnQualifier() {
    final KijiURI uri =
        KijiURI.newBuilder("kiji-hbase://zkhost/instance/table/map:one%20two").build();
    assertEquals("map:one two", uri.getColumns().get(0).getName());
  }

  @Test
  public void testConstructedUriIsEscaped() {
    // SCHEMA-6. Column qualifier must be URL-encoded in KijiURI.
    final KijiURI uri = KijiURI.newBuilder("kiji-hbase://zkhost/instance/table/")
        .addColumnName(KijiColumnName.create("map:one two")).build();
    assertEquals("kiji-hbase://zkhost:2181/instance/table/map:one%20two/", uri.toString());
  }

}

