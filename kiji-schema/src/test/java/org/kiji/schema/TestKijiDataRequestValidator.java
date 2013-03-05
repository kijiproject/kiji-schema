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

import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;


public class TestKijiDataRequestValidator extends KijiClientTest {
  private KijiTableLayout mTableLayout;

  @Before
  public void setupLayout() throws Exception {
    mTableLayout =
        KijiTableLayouts.getTableLayout(KijiTableLayouts.FULL_FEATURED);
    getKiji().createTable(mTableLayout.getDesc());
  }

  @Test
  public void testValidate() throws InvalidLayoutException {
    KijiDataRequestBuilder builder = KijiDataRequest.builder().withTimeRange(2, 3);
    builder.newColumnsDef().withMaxVersions(1).add("info", "name");
    KijiDataRequest request = builder.build();

    KijiDataRequestValidator validator = new KijiDataRequestValidator(request);
    validator.validate(mTableLayout);
  }

  @Test(expected=KijiDataRequestException.class)
  public void testValidateNoSuchFamily() throws InvalidLayoutException {
    KijiDataRequestBuilder builder = KijiDataRequest.builder().withTimeRange(2, 3);
    builder.newColumnsDef().withMaxVersions(1).add("blahblah", "name");
    KijiDataRequest request = builder.build();

    KijiDataRequestValidator validator = new KijiDataRequestValidator(request);
    validator.validate(mTableLayout);
  }

  @Test(expected=KijiDataRequestException.class)
  public void testValidateNoSuchColumn() throws InvalidLayoutException {
    KijiDataRequestBuilder builder = KijiDataRequest.builder().withTimeRange(2, 3);
    builder.newColumnsDef().withMaxVersions(1)
        .add("info", "name")
        .add("info", "blahblah");
    KijiDataRequest request = builder.build();

    KijiDataRequestValidator validator = new KijiDataRequestValidator(request);
    validator.validate(mTableLayout);
  }
}
