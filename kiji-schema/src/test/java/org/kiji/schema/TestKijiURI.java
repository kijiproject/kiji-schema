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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.junit.Test;

public class TestKijiURI {
  @Test
  public void testFromURI() throws KijiURIException, URISyntaxException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost:1234");
    assertEquals(uri, KijiURI.fromURI(new URI(uri.toString())));
  }

  @Test
  public void testHbaseUri() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost:1234");
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals(null, uri.getInstance());
    assertEquals(null, uri.getTable());
    assertTrue(uri.getColumns().isEmpty());
  }

  @Test
  public void testKijiInstanceUri() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost:1234/instance");
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals(null, uri.getTable());
    assertTrue(uri.getColumns().isEmpty());
  }

  @Test
  public void testSingleHost() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost:1234/instance/table/col");
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testSingleHostGroupColumn() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost:1234/instance/table/family:qualifier");
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("family:qualifier", uri.getColumns().get(0).getName());
  }

  @Test
  public void testSingleHostDefaultInstance() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost:1234/default/table/col");
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1, uri.getZookeeperQuorum().size());
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("default", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testSingleHostDefaultPort() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost/instance/table/col");
    assertEquals(1, uri.getZookeeperQuorum().size());
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(KijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testMultipleHosts() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://(zkhost1,zkhost2):1234/instance/table/col");
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testMultipleHostsDefaultPort() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost1,zkhost2/instance/table/col");
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(KijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testMultipleHostsDefaultPortDefaultInstance() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost1,zkhost2/default/table/col");
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(KijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("default", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test(expected = KijiURIException.class)
  public void testMultipleHostsNoParen() throws KijiURIException {
    KijiURI.parse("kiji://zkhost1,zkhost2:1234/instance/table/col");
  }

  @Test(expected = KijiURIException.class)
  public void testMultipleHostsMultiplePorts() throws KijiURIException {
    KijiURI.parse("kiji://zkhost1:1234,zkhost2:2345/instance/table/col");
  }

  @Test
  public void testMultipleColumns() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost1,zkhost2/default/table/col1,col2");
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(KijiURI.DEFAULT_ZOOKEEPER_CLIENT_PORT, uri.getZookeeperClientPort());
    assertEquals("default", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals(2, uri.getColumns().size());
  }

  @Test(expected = KijiURIException.class)
  public void testExtraPath() throws KijiURIException {
    KijiURI.parse("kiji://(zkhost1,zkhost2):1234/instance/table/col/extra");
  }

  @Test
  public void testURIWithQuery() throws KijiURIException {
    final KijiURI uri =
        KijiURI.parse("kiji://(zkhost1,zkhost2):1234/instance/table/col?query");
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testURIWithFragment() throws KijiURIException {
    final KijiURI uri =
        KijiURI.parse("kiji://(zkhost1,zkhost2):1234/instance/table/col#frag");
    assertEquals("zkhost1", uri.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", uri.getZookeeperQuorum().get(1));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals("instance", uri.getInstance());
    assertEquals("table", uri.getTable());
    assertEquals("col", uri.getColumns().get(0).getName());
  }

  @Test
  public void testPartialURIZookeeper() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost:1234");
    assertEquals("zkhost", uri.getZookeeperQuorum().get(0));
    assertEquals(1234, uri.getZookeeperClientPort());
    assertEquals(null, uri.getInstance());
  }

  @Test
  public void testBasicResolution() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost:1234");
    final KijiURI resolved = uri.resolve("testinstance");
    assertEquals("testinstance", resolved.getInstance());
  }

  @Test
  public void testResolution() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost:1234/.unset");
    final KijiURI resolved = uri.resolve("testinstance");
    assertEquals("testinstance", resolved.getInstance());
  }

  @Test
  public void testResolutionColumn() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost/instance/table");
    final KijiURI resolved = uri.resolve("col");
    assertEquals("col", resolved.getColumns().get(0).getName());
  }

  @Test(expected = KijiURIException.class)
  public void testInvalidResolution() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost:1234");
     uri.resolve("instance/table/col/extra");
  }

  @Test
  public void testToString() throws KijiURIException {
    String uri = "kiji://(zkhost1,zkhost2):1234/instance/table/col/";
    assertEquals(uri, KijiURI.parse(uri).toString());
    uri = "kiji://zkhost1:1234/instance/table/col/";
    assertEquals(uri, KijiURI.parse(uri).toString());
    uri = "kiji://zkhost:1234/instance/table/col1,col2/";
    assertEquals(uri, KijiURI.parse(uri).toString());
    uri = "kiji://zkhost:1234/.unset/table/col/";
    assertEquals(uri, KijiURI.parse(uri).toString());
  }

  @Test
  public void testNormalizedQuorum() throws KijiURIException {
    KijiURI uri = KijiURI.parse("kiji://(zkhost1,zkhost2):1234/instance/table/col/");
    KijiURI reversedQuorumUri = KijiURI.parse("kiji://(zkhost2,zkhost1):1234/instance/table/col/");
    assertEquals(uri.toString(), reversedQuorumUri.toString());
    assertEquals(uri.getZookeeperQuorum(), reversedQuorumUri.getZookeeperQuorum());
  }

  @Test
  public void testNormalizedColumns() throws KijiURIException {
    KijiURI uri = KijiURI.parse("kiji://(zkhost1,zkhost2):1234/instance/table/col/");
    KijiURI reversedColumnURI = KijiURI.parse("kiji://(zkhost2,zkhost1):1234/instance/table/col/");
    assertEquals(uri.toString(), reversedColumnURI.toString());
    assertEquals(uri.getColumns(), reversedColumnURI.getColumns());
  }

  @Test
  public void testOrderedQuorum() throws KijiURIException {
    KijiURI uri = KijiURI.parse("kiji://(zkhost1,zkhost2):1234/instance/table/col/");
    KijiURI reversedQuorumUri = KijiURI.parse("kiji://(zkhost2,zkhost1):1234/instance/table/col/");
    assertFalse(uri.getZookeeperQuorumOrdered()
        .equals(reversedQuorumUri.getZookeeperQuorumOrdered()));
    assertFalse(uri.toOrderedString().equals(reversedQuorumUri.toOrderedString()));
  }

  @Test
  public void testOrderedColumns() throws KijiURIException {
    KijiURI uri = KijiURI.parse("kiji://(zkhost1,zkhost2):1234/instance/table/col1,col2/");
    KijiURI reversedColumnURI =
        KijiURI.parse("kiji://(zkhost1,zkhost2):1234/instance/table/col2,col1/");
    assertFalse(uri.toOrderedString().equals(reversedColumnURI.toOrderedString()));
    assertFalse(uri.getColumnsOrdered().equals(reversedColumnURI.getColumnsOrdered()));
  }

  @Test
  public void testKijiURIBuilderFromInstance() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost:1234/.unset/table");
    KijiURI built = uri.newBuilder().build();
    assertEquals(uri, built);
  }

  @Test
  public void testKijiURIBuilderWithInstance() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost:1234/instance1/table");
    assertEquals("instance1", uri.getInstance());
    final KijiURI modified =
        uri.newBuilder().withInstanceName("instance2").build();
    assertEquals("instance2", modified.getInstance());
    assertEquals("instance1", uri.getInstance());
  }

  @Test
  public void testSetColumn() throws KijiURIException {
    KijiURI uri = KijiURI.parse("kiji://zkhost/instance/table/");
    assertTrue(uri.getColumns().isEmpty());
    uri =
        uri.newBuilder().withColumnNames(Arrays.asList("testcol1", "testcol2"))
        .build();
    assertEquals(2, uri.getColumns().size());
  }

  @Test
  public void testSetZookeeperQuorum() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost/instance/table/col");
    final KijiURI modified = uri.newBuilder()
        .withZookeeperQuorum(new String[] {"zkhost1", "zkhost2"}).build();
    assertEquals(2, modified.getZookeeperQuorum().size());
    assertEquals("zkhost1", modified.getZookeeperQuorum().get(0));
    assertEquals("zkhost2", modified.getZookeeperQuorum().get(1));
  }

  @Test
  public void testTrailingUnset() throws KijiURIException {
    final KijiURI uri = KijiURI.parse("kiji://zkhost/.unset/table/.unset");
    KijiURI result = uri.newBuilder().withTableName(".unset").build();
    assertEquals("kiji://zkhost:2181/", result.toString());
  }
}

