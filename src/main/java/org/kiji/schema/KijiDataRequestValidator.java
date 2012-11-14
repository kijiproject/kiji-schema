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

import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * This class validates a {@link KijiDataRequest} against the layout
 * of a Kiji table to make sure it contains all of the columns requested.
 */
public class KijiDataRequestValidator {
  /** The Kiji data request to validate. */
  private KijiDataRequest mDataRequest;

  /**
   * Construct a validator for a data request.
   *
   * @param dataRequest The data request to validate.
   */
  public KijiDataRequestValidator(KijiDataRequest dataRequest) {
    mDataRequest = dataRequest;
  }

  /**
   * Validates the data request against the given table layout.
   *
   * @param tableLayout The Kiji table layout to validate against.
   * @throws InvalidLayoutException If the table layout is invalid.
   * @throws KijiDataRequestException If the data request is invalid.
   */
  public void validate(KijiTableLayout tableLayout) throws InvalidLayoutException {
    for (KijiDataRequest.Column wdrColumn : mDataRequest.getColumns()) {
      final String qualifier = wdrColumn.getKey();
      final KijiTableLayout.LocalityGroupLayout.FamilyLayout fLayout =
          tableLayout.getFamilyMap().get(wdrColumn.getFamily());

      if (null == fLayout) {
        throw new KijiDataRequestException(String.format("Table '%s' has no family named '%s'.",
            tableLayout.getName(), wdrColumn.getFamily()));
      }

      if (fLayout.isGroupType() && (null != wdrColumn.getKey())) {
        if (!fLayout.getColumnMap().containsKey(qualifier)) {
          throw new KijiDataRequestException(String.format("Table '%s' has no column '%s'.",
              tableLayout.getName(), wdrColumn.getName()));
        }
      }
    }
  }
}
