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

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * This class validates a {@link KijiDataRequest} against the layout
 * of a Kiji table to make sure it contains all of the columns requested.
 *
 * This class cannot be instantiated. Instead, programs should use the static
 * {@link validate(KijiDataRequest dataRequest, KijiTableLayout tableLayout)} method to validate
 * requests against layouts.
 */
@ApiAudience.Framework
public final class KijiDataRequestValidator {
  /**
   * Private empty constructor to ensure this class is never instantiated.
   */
  private KijiDataRequestValidator() { }

  /**
   * Validates a data request against the given table layout.
   *
   * @param dataRequest The KijiDataRequest to validate.
   * @param tableLayout The Kiji table layout to validate against.
   * @throws KijiDataRequestException If the data request is invalid.
   */
  public static void validate(KijiDataRequest dataRequest, KijiTableLayout tableLayout) {
    if (null == dataRequest) {
      throw new KijiDataRequestException("Data request cannot be null.");
    }

    for (KijiDataRequest.Column column : dataRequest.getColumns()) {
      final String qualifier = column.getQualifier();
      final KijiTableLayout.LocalityGroupLayout.FamilyLayout fLayout =
          tableLayout.getFamilyMap().get(column.getFamily());

      if (null == fLayout) {
        throw new KijiDataRequestException(String.format("Table '%s' has no family named '%s'.",
            tableLayout.getName(), column.getFamily()));
      }

      if (fLayout.isGroupType() && (null != column.getQualifier())) {
        if (!fLayout.getColumnMap().containsKey(qualifier)) {
          throw new KijiDataRequestException(String.format("Table '%s' has no column '%s'.",
              tableLayout.getName(), column.getName()));
        }
      }
    }
  }
}
