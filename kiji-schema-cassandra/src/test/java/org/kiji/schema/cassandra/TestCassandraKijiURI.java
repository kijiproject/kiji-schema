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

package org.kiji.schema.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.Test;

import org.kiji.schema.KConstants;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiURIException;

public class TestCassandraKijiURI {
  @Test
  public void testCassandraUri() {
    final CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost:1234/chost:5678").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals(null, uri.getInstance());
    assertEquals(null, uri.getTable());
    assertTrue(uri.getColumns().isEmpty());
  }

  @Test
  public void testKijiInstanceUri() {
    final CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost:1234/chost:5678/instance").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("instance", uri.getInstance());
    assertEquals(null, uri.getTable());
    assertTrue(uri.getColumns().isEmpty());
  }

  @Test
  public void testSingleHost() {
    final CassandraKijiURI uri = CassandraKijiURI
        .newBuilder("kiji-cassandra://zkhost:1234/chost:5678/instance/table/col").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testSingleHostGroupColumn() {
    final CassandraKijiURI uri =
        CassandraKijiURI.newBuilder(
            "kiji-cassandra://zkhost:1234/chost:5678/instance/table/family:qualifier").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("family:qualifier", uri.getColumns().get(0).getName());
  }

  @Test
  public void testSingleHostDefaultInstance() {
    final CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost:1234/chost:5678/default/table/col").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1, uri.getZookeeperQuorum().size());
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("default", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testSingleHostDefaultPort() {
    final CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost/chost:5678/instance/table/col").build();
    assertEquals(1, uri.getZookeeperQuorum().size());
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(CassandraKijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testMultipleHosts() {
    final CassandraKijiURI uri = CassandraKijiURI
        .newBuilder(
            "kiji-cassandra://(zkhost1,zkhost2):1234/(chost1,chost2):5678/instance/table/col")
        .build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("chost1", uri.getContactPoints().get(0));
    assertEquals("chost2", uri.getContactPoints().get(1));
    assertEquals(5678, uri.getContactPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testMultipleHostsDefaultPort() {
    final CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost1,zkhost2/chost:5678/instance/table/col").build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(CassandraKijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testMultipleHostsDefaultPortDefaultInstance() {
    final CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost1,zkhost2/chost:5678/default/table/col").build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(CassandraKijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("chost", uri.getContactPoints().get(0));
    assertEquals(5678, uri.getContactPort());
    assertEquals("default", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testRelativeToDefaultURI() {
    final CassandraKijiURI uri = CassandraKijiURI.newBuilder("instance/table/col").build();
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
    final CassandraKijiURI uriTwo =
        CassandraKijiURI.newBuilder("///instance///table/////col").build();
    assertEquals("instance", uriTwo.getInstance());
    assertEquals("table", uriTwo.getTable());
    assertEquals("col", uriTwo.getColumns().get(0).getName());
  }

  @Test
  public void testNoAuthority() {
    try {
      CassandraKijiURI.newBuilder("kiji-cassandra:///");
      fail("An exception should have been thrown.");
    } catch (KijiURIException kurie) {
      assertEquals("Invalid Kiji URI: 'kiji-cassandra:///' : ZooKeeper ensemble missing.",
          kurie.getMessage());
    }
  }

  @Test
  public void testBrokenCassandraPort() {
    try {
      CassandraKijiURI.newBuilder("kiji-cassandra://zkhost/chost:port/default").build();
      fail("An exception should have been thrown.");
    } catch (KijiURIException kurie) {
      assertEquals(
          "Invalid Kiji URI: 'kiji-cassandra://zkhost/chost:port/default' "
              + ": Can not parse port 'port'.",
          kurie.getMessage());
    }
  }

  @Test
  public void testMultipleHostsNoParen() {
    try {
      CassandraKijiURI.newBuilder(
          "kiji-cassandra://zkhost1,zkhost2:1234/chost:5678/instance/table/col");
      fail("An exception should have been thrown.");
    } catch (KijiURIException kurie) {
      assertEquals(
          "Invalid Kiji URI: 'kiji-cassandra://zkhost1,zkhost2:1234/chost:5678/instance/table/col'"
              + " : Multiple ZooKeeper hosts must be parenthesized.", kurie.getMessage());
    }
  }

  @Test
  public void testMultipleHostsMultiplePorts() {
    try {
      CassandraKijiURI.newBuilder(
          "kiji-cassandra://zkhost1:1234,zkhost2:2345/chost:5678/instance/table/col");
      fail("An exception should have been thrown.");
    } catch (KijiURIException kurie) {
      assertEquals(
          "Invalid Kiji URI: 'kiji-cassandra://zkhost1:1234,zkhost2:2345/chost:5678/"
              + "instance/table/col' : Invalid ZooKeeper ensemble cluster identifier.",
          kurie.getMessage());
    }
  }

  @Test
  public void testMultipleColumns() {
    final CassandraKijiURI uri =
        CassandraKijiURI.newBuilder(
            "kiji-cassandra://zkhost1,zkhost2/chost:5678/default/table/col1,col2").build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(CassandraKijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("default", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals(2, uri.getColumns().size());
  }

  @Test
  public void testExtraPath() {
    try {
      CassandraKijiURI.newBuilder(
          "kiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col/extra");
      fail("An exception should have been thrown.");
    } catch (KijiURIException kurie) {
      assertEquals(
          "Invalid Kiji URI: 'kiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/"
              + "instance/table/col/extra' : Too many path segments.",
          kurie.getMessage());
    }
  }

  @Test
  public void testURIWithQuery() {
    final CassandraKijiURI uri =
        CassandraKijiURI.newBuilder(
            "kiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col?query").build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testURIWithFragment() {
    final CassandraKijiURI uri =
        CassandraKijiURI.newBuilder(
            "kiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col#frag").build();
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testPartialURIZookeeper() {
    final CassandraKijiURI uri =
        CassandraKijiURI.newBuilder("kiji-cassandra://zkhost:1234/chost:5678").build();
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals(null, uri.getInstance());
  }

  @Test
  public void testBasicResolution() {
    final CassandraKijiURI uri =
        CassandraKijiURI.newBuilder("kiji-cassandra://zkhost:1234/chost:5678").build();
    final CassandraKijiURI resolved = uri.resolve("testinstance");
    assertEquals("testinstance", resolved.getInstance());
  }

  @Test
  public void testResolution() {
    final CassandraKijiURI uri =
        CassandraKijiURI.newBuilder("kiji-cassandra://zkhost:1234/chost:5678/.unset").build();
    final CassandraKijiURI resolved = uri.resolve("testinstance");
    assertEquals("testinstance", resolved.getInstance());
  }

  @Test
  public void testResolutionColumn() {
    final CassandraKijiURI uri =
        CassandraKijiURI.newBuilder("kiji-cassandra://zkhost/chost:5678/instance/table").build();
    final CassandraKijiURI resolved = uri.resolve("col");
    assertEquals("col", resolved.getColumns().get(0).getName());
  }

  @Test
  public void testInvalidResolution() {
    final CassandraKijiURI uri =
        CassandraKijiURI.newBuilder("kiji-cassandra://zkhost:1234/chost:5678").build();
    try {
      uri.resolve("instance/table/col/extra");
      fail("An exception should have been thrown.");
    } catch (KijiURIException kurie) {
      assertEquals("Invalid Kiji URI: "
          + "'kiji-cassandra://zkhost:1234/chost:5678/instance/table/col/extra' : "
              + "Too many path segments.", kurie.getMessage());
    }
  }

  @Test
  public void testToString() {
    String uri = "kiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col/";
    assertEquals(uri, CassandraKijiURI.newBuilder(uri).build().toString());
    uri = "kiji-cassandra://zkhost1:1234/chost:5678/instance/table/col/";
    assertEquals(uri, CassandraKijiURI.newBuilder(uri).build().toString());
    uri = "kiji-cassandra://zkhost:1234/chost:5678/instance/table/col1,col2/";
    assertEquals(uri, CassandraKijiURI.newBuilder(uri).build().toString());
    uri = "kiji-cassandra://zkhost:1234/chost:5678/.unset/table/col/";
    assertEquals(uri, CassandraKijiURI.newBuilder(uri).build().toString());
  }

  @Test
  public void testNormalizedQuorum() {
    CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col/").build();
    CassandraKijiURI reversedQuorumUri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://(zkhost2,zkhost1):1234/chost:5678/instance/table/col/").build();
    assertEquals(uri.toString(), reversedQuorumUri.toString());
    assertEquals(uri.getZookeeperQuorum(), reversedQuorumUri.getZookeeperQuorum());
  }

  @Test
  public void testNormalizedCassandraNodes() {
    CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost:1234/(chost1,chost2):5678/instance/table/col/").build();
    CassandraKijiURI reversedUri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost:1234/(chost2,chost1):5678/instance/table/col/").build();
    assertEquals(uri.toString(), reversedUri.toString());
    assertEquals(uri.getContactPoints(), reversedUri.getContactPoints());
  }

  @Test
  public void testNormalizedColumns() {
    CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col/").build();
    CassandraKijiURI reversedColumnURI = CassandraKijiURI.newBuilder(
        "kiji-cassandra://(zkhost2,zkhost1):1234/chost:5678/instance/table/col/").build();
    assertEquals(uri.toString(), reversedColumnURI.toString());
    assertEquals(uri.getColumns(), reversedColumnURI.getColumns());
  }

  @Test
  public void testOrderedQuorum() {
    CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col/").build();
    CassandraKijiURI reversedQuorumUri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://(zkhost2,zkhost1):1234/chost:5678/instance/table/col/").build();
    assertFalse(uri.getZookeeperQuorumOrdered()
        .equals(reversedQuorumUri.getZookeeperQuorumOrdered()));
    assertFalse(uri.toOrderedString().equals(reversedQuorumUri.toOrderedString()));
  }

  @Test
  public void testOrderedCassandraNodes() {
    String revString =  "kiji-cassandra://zkhost:1234/(chost2,chost1):5678/instance/table/col/";
    String ordString =  "kiji-cassandra://zkhost:1234/(chost1,chost2):5678/instance/table/col/";
    CassandraKijiURI revUri = CassandraKijiURI.newBuilder(revString).build();

    // "toString" should ignore the user-defined ordering.
    assertEquals(ordString, revUri.toString());

    // "toOrderedString" should maintain the user-defined ordering.

  }
  @Test
  public void testOrderedColumns() {
    CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col1,col2/").build();
    CassandraKijiURI reversedColumnURI = CassandraKijiURI.newBuilder(
        "kiji-cassandra://(zkhost1,zkhost2):1234/chost:5678/instance/table/col2,col1/").build();
    assertFalse(uri.toOrderedString().equals(reversedColumnURI.toOrderedString()));
    assertFalse(uri.getColumnsOrdered().equals(reversedColumnURI.getColumnsOrdered()));
  }

  /**
   * Tests that CassandraKijiURI.newBuilder().build() builds a URI for the default Kiji instance
   * URI.
   *
   * The default Kiji instance URI is environment specific. Hence, this cannot test for explicit
   * values of the ZooKeeper quorum of of the ZooKeeper client port.
   */
  @Test
  public void testCassandraKijiURIBuilderDefault() {
    CassandraKijiURI uri = CassandraKijiURI.newBuilder().build();
    assertTrue(!uri.getZookeeperQuorum().isEmpty());  // Test cannot be more specific.
    // Test cannot validate the value of uri.getZookeeperClientPort().
    assertEquals(KConstants.DEFAULT_INSTANCE_NAME, uri.getInstance());
    assertEquals(null, uri.getTable());
    assertTrue(uri.getColumns().isEmpty());
  }

  @Test
  public void testCassandraKijiURIBuilderFromInstance() {
    final CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost:1234/chost:5678/.unset/table").build();
    CassandraKijiURI built = CassandraKijiURI.newBuilder(uri).build();
    assertEquals(uri, built);
  }

  @Test
  public void testCassandraKijiURIBuilderWithInstance() {
    final CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost:1234/chost:5678/instance1/table").build();
    assertEquals("instance1", uri.getInstance());
    final CassandraKijiURI modified =
        CassandraKijiURI.newBuilder(uri).withInstanceName("instance2").build();
    assertEquals("instance2", modified.getInstance());
    assertEquals("instance1", uri.getInstance());
  }

  @Test
  public void testSetColumn() {
    CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost/chost:5678/instance/table/").build();
    assertTrue(uri.getColumns().isEmpty());
    uri =
        CassandraKijiURI.newBuilder(uri).withColumnNames(Arrays.asList("testcol1", "testcol2"))
        .build();
    assertEquals(2, uri.getColumns().size());
  }

  @Test
  public void testSetZookeeperQuorum() {
    final CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost/chost:5678/instance/table/col").build();
    final CassandraKijiURI modified = CassandraKijiURI.newBuilder(uri)
        .withZookeeperQuorum(new String[] {"zkhost1", "zkhost2"}).build();
    assertEquals(2, modified.getZookeeperQuorum().size());
    assertEquals("zkhost1", modified.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", modified.getZookeeperQuorum().get(1));
  }

  @Test
  public void testTrailingUnset() {
    final CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost/chost:5678/.unset/table/.unset").build();
    CassandraKijiURI result = CassandraKijiURI.newBuilder(uri).withTableName(".unset").build();
    assertEquals("kiji-cassandra://zkhost:2181/chost:5678/", result.toString());
  }

  @Test
  public void testEscapedMapColumnQualifier() {
    final CassandraKijiURI uri = CassandraKijiURI.newBuilder(
        "kiji-cassandra://zkhost/chost:5678/instance/table/map:one%20two").build();
    assertEquals("map:one two", uri.getColumns().get(0).getName());
  }

  @Test
  public void testConstructedUriIsEscaped() {
    // SCHEMA-6. Column qualifier must be URL-encoded in CassandraKijiURI.
    final CassandraKijiURI uri =
        CassandraKijiURI.newBuilder("kiji-cassandra://zkhost/chost:5678/instance/table/")
        .addColumnName(new KijiColumnName("map:one two")).build();
    assertEquals(
        "kiji-cassandra://zkhost:2181/chost:5678/instance/table/map:one%20two/", uri.toString());
  }
}
