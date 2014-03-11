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

package org.kiji.schema.security;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.security.access.Permission;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/**
 * Encapsulates the permissions state for a user of a Kiji instance.
 *
 * <p>Instances of KijiPermissions are immutable.  All methods that change the allowed actions
 * return a new instance of KijiPermissions with the new actions.</p>
 *
 * This class may be modified later to also represent the permissions of Kiji tables, columns, or
 * rows.
 */
@ApiAudience.Framework
@ApiStability.Experimental
public final class KijiPermissions {
  /** Actions in Kiji that have an HBase Action counterpart. */
  private static final EnumSet<Action> HBASE_ACTIONS = EnumSet.of(
      Action.READ,
      Action.WRITE,
      Action.GRANT);

  /** A singleton empty permissions object. */
  private static final KijiPermissions EMPTY_PERMISSIONS_SINGLETON =
      new KijiPermissions(EnumSet.noneOf(Action.class));

  /** All actions allowed by this KijiPermissions object. */
  private final Set<Action> mActions;

  /**
   * Constructs a new KijiPermissions object with the actions specified.
   *
   * @param actions to permit.
   */
  private KijiPermissions(Set<Action> actions) {
    mActions = EnumSet.copyOf(actions);
  }

  /**
   * Constructs a new KijiPermissions with no permitted actions.
   *
   * @return a new KijiPermissions objects with no permitted actions.
   */
  public static KijiPermissions emptyPermissions() {
    return EMPTY_PERMISSIONS_SINGLETON;
  }

  /**
   * Constructs a new KijiPermissions with the specified actions.
   *
   * @param actions allowed by the KijiPermissions returned.
   * @return a KijiPermissions with the specified actions.
   */
  public static KijiPermissions newWithActions(Set<Action> actions) {
    return new KijiPermissions(actions);
  }

  /** All possible actions in Kiji. */
  public static enum Action {
    READ(Permission.Action.READ, "kiji.security-0.1.permission.read"),
    WRITE(Permission.Action.WRITE, "kiji.security-0.1.permission.write"),
    GRANT(Permission.Action.ADMIN, "kiji.security-0.1.permission.grant");

    /** The corresponding HBase Permission.Action.  Null if no corresponding HBase Action. */
    private final Permission.Action mHBaseAction;

    /** The string used as an HBase row key used to store users with this Action in Kiji. */
    private final String mStringKey;

    /**
     * Gets the string used as an HBase row key to store users with this Action in Kiji.
     *
     * @return the string used as an HBase row key to store users with this Action in Kiji.
     */
    protected String getStringKey() {
      return mStringKey;
    }

    /**
     * Construct a new Kiji Action.
     *
     * @param hBaseAction The corresponding HBase Permission.Action. Null if no corresponding
     *     HBase Action.
     * @param stringKey The string used as an HBase row key to store users with this Action in
     *     Kiji.
     */
    private Action(Permission.Action hBaseAction, String stringKey) {
      mHBaseAction = hBaseAction;
      mStringKey = stringKey;
    }

    /**
     * Gets the corresponding HBase Permission.Action, or null if there is none.
     *
     * @return The corresponding HBase Permission.Action, or null if there is none.
     */
    private Permission.Action getHBaseAction() {
      return mHBaseAction;
    }
  }

  /**
   * Constructs a new KijiPermissions object with the specified action added to the current
   * permissions.
   *
   * @param action to add.
   * @return a new KijiPermissions with 'action' added.
   */
  public KijiPermissions addAction(Action action) {
    Preconditions.checkNotNull(action);
    Set<Action> newActions = EnumSet.copyOf(mActions);
    newActions.add(action);
    return new KijiPermissions(newActions);
  }

  /**
   * Constructs a new KijiPermissions object with the specified action removed from the current
   * permissions.  If 'action' is not in the current permissions, returns a new KijiPermissions
   * with the same permissions.
   *
   * @param action to remove.
   * @return a new KijiPermissions with 'action' removed, if it was present.
   */
  public KijiPermissions removeAction(Action action) {
    Preconditions.checkNotNull(action);
    Set<Action> newActions = EnumSet.copyOf(mActions);
    newActions.remove(action);
    return new KijiPermissions(newActions);
  }

  /**
   * Gets a copy of the actions allowed by this KijiPermissions.
   *
   * @return the actions allowed by this KijiPermissions.
   */
  public Set<Action> getActions() {
    return EnumSet.copyOf(mActions);
  }

  /**
   * Returns true if this KijiPermissions allows the action, false otherwise.
   *
   * @param action to check permissions for.
   * @return true if this Kiji Permissions allows the action, false otherwise.
   */
  public boolean allowsAction(Action action) {
    Preconditions.checkNotNull(action);
    return mActions.contains(action);
  }

  /**
   * Returns an array of all HBase Actions specified by this KijiPermissions object. This is used
   * internally to apply the correct permissions on HBase tables.
   *
   * @return HBase Actions for all of the permitted Actions with corresponding HBase Actions.
   */
  protected Permission.Action[] toHBaseActions() {
    final Set<Action> mHBaseActions = Sets.intersection(mActions, HBASE_ACTIONS);
    final Set<Permission.Action> convertedActions = EnumSet.noneOf(Permission.Action.class);
    for (Action kijiAction : mHBaseActions) {
      convertedActions.add(kijiAction.getHBaseAction());
    }
    return convertedActions.toArray(new Permission.Action[convertedActions.size()]);
  }
}
