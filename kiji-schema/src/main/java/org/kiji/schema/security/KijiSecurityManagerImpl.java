/**
 * (c) Copyright 2013 WibiData, Inc.
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.impl.Versions;
import org.kiji.schema.impl.hbase.HBaseKiji;
import org.kiji.schema.util.Lock;
import org.kiji.schema.zookeeper.ZooKeeperLock;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/**
 * The default implementation of KijiSecurityManager.
 *
 * <p>KijiSecurityManager manages access control for a Kiji instance.  It depends on ZooKeeper
 * locks to ensure atomicity of permissions operations.</p>
 *
 * <p>The current version of Kiji security (security-0.1) is instance-level only.  Users can have
 * READ, WRITE, and/or GRANT access on a Kiji instance.</p>
 */
@ApiAudience.Private
final class KijiSecurityManagerImpl implements KijiSecurityManager {
  private static final Logger LOG = LoggerFactory.getLogger(KijiSecurityManagerImpl.class);

  /** The Kiji instance this manages. */
  private final KijiURI mInstanceUri;

  /** A handle to the Kiji this manages. */
  private final Kiji mKiji;

  /** A handle to the HBaseAdmin of mKiji. */
  private final HBaseAdmin mAdmin;

  /** The system table for the instance this manages. */
  private final KijiSystemTable mSystemTable;

  /** The HBase ACL (Access Control List) table to use. */
  private final HTableInterface mAccessControlTable;

  /** ZooKeeper client connection responsible for creating instance locks. */
  private final CuratorFramework mZKClient;

  /** The zookeeper lock for this instance. */
  private final Lock mLock;

  /** The timeout, in seconds, to wait for ZooKeeper locks before throwing an exception. */
  private static final int LOCK_TIMEOUT = 10;

  /**
   * Constructs a new KijiSecurityManager for an instance, with the specified configuration.
   *
   * <p>A KijiSecurityManager cannot be constructed for an instance if the instance does not have a
   * security version greater than or equal to security-0.1 (that is, if security is not enabled).
   * </p>
   *
   * @param instanceUri is the URI of the instance this KijiSecurityManager will manage.
   * @param conf is the Hadoop configuration to use.
   * @param tableFactory is the table factory to use to access the HBase ACL table.
   * @throws IOException on I/O error.
   * @throws KijiSecurityException if the Kiji security version is not compatible with
   *     KijiSecurityManager.
   */
  KijiSecurityManagerImpl(
      KijiURI instanceUri,
      Configuration conf,
      HTableInterfaceFactory tableFactory) throws IOException {
    mInstanceUri = instanceUri;
    mKiji = Kiji.Factory.get().open(mInstanceUri);
    mSystemTable = mKiji.getSystemTable();

    // If the Kiji has security version lower than MIN_SECURITY_VERSION, then KijiSecurityManager
    // can't be instantiated.
    if (mSystemTable.getSecurityVersion().compareTo(Versions.MIN_SECURITY_VERSION) < 0) {
      mKiji.release();
      throw new KijiSecurityException("Cannot create a KijiSecurityManager for security version "
          + mSystemTable.getSecurityVersion() + ". Version must be "
          + Versions.MIN_SECURITY_VERSION + " or higher.");
    }

    mAdmin = ((HBaseKiji) mKiji).getHBaseAdmin();

    // TODO(SCHEMA-921): Security features should be moved into a bridge for CDH4.
    // Get the access control table.
    mAccessControlTable = tableFactory
        .create(conf, AccessControlLists.ACL_TABLE_NAME.getNameAsString());

    mZKClient = ZooKeeperUtils.getZooKeeperClient(mInstanceUri);
    mLock = new ZooKeeperLock(mZKClient, ZooKeeperUtils.getInstancePermissionsLock(instanceUri));
  }

  /** {@inheritDoc} */
  @Override
  public void lock() throws IOException {
    LOG.debug("Locking permissions for instance: '{}'.", mInstanceUri);
    boolean lockSuccessful = mLock.lock(LOCK_TIMEOUT);
    if (!lockSuccessful) {
      throw new KijiSecurityException("Acquiring lock on instance " + mInstanceUri
          + " timed out after " + LOCK_TIMEOUT + " seconds.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void unlock() throws IOException {
    LOG.debug("Unlocking permissions for instance: '{}'.", mInstanceUri);
    mLock.unlock();
  }

  /** {@inheritDoc} */
  @Override
  public void grant(KijiUser user, KijiPermissions.Action action)
      throws IOException {
    lock();
    try {
      grantWithoutLock(user, action);
    } finally {
      unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void grantAll(KijiUser user) throws IOException {
    lock();
    try {
      grantAllWithoutLock(user);
    } finally {
      unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void revoke(KijiUser user, KijiPermissions.Action action)
      throws IOException {
    lock();
    try {
      KijiPermissions currentPermissions = getPermissions(user);
      KijiPermissions newPermissions = currentPermissions.removeAction(action);
      updatePermissions(user, newPermissions);
      revokeInstancePermissions(user, KijiPermissions.newWithActions(Sets.newHashSet(action)));
    } finally {
      unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void revokeAll(KijiUser user) throws IOException {
    lock();
    try {
      updatePermissions(user, KijiPermissions.emptyPermissions());
      revokeInstancePermissions(
          user, KijiPermissions.newWithActions(Sets.newHashSet(KijiPermissions.Action.values())));
    } finally {
      unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void reapplyInstancePermissions() throws IOException {
    lock();
    try {
      Set<KijiUser> allUsers = listAllUsers();
      for (KijiUser user : allUsers) {
        KijiPermissions permissions = getPermissions(user);
        // Grant privileges the user should have.
        for (KijiPermissions.Action action : permissions.getActions()) {
          grant(user, action);
        }
        // Revoke privileges the user shouldn't have.
        Set<KijiPermissions.Action> forbiddenActions =
            Sets.difference(
                Sets.newHashSet(KijiPermissions.Action.values()),
                permissions.getActions());
        for (KijiPermissions.Action action : forbiddenActions) {
          revoke(user, action);
        }
      }
    } finally {
      unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void applyPermissionsToNewTable(KijiURI tableURI) throws IOException {
    // The argument must be for a table in the instance this manages.
    Preconditions.checkArgument(
        KijiURI.newBuilder(mInstanceUri).withTableName(tableURI.getTable()).build()
            .equals(tableURI));
    for (KijiUser user : listAllUsers()) {
      grantHTablePermissions(user.getName(),
          KijiManagedHBaseTableName
              .getKijiTableName(tableURI.getInstance(), tableURI.getTable()).toBytes(),
          getPermissions(user).toHBaseActions());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void grantInstanceCreator(KijiUser user) throws IOException {
    lock();
    try {
      Set<KijiUser> currentGrantors = getUsersWithPermission(KijiPermissions.Action.GRANT);
      // This can only be called if there are no grantors, right when the instance is created.
      if (currentGrantors.size() != 0) {
        throw new KijiAccessException(
            "Cannot add user " + user
                + " to grantors as the instance creator for instance '"
                + mInstanceUri.toOrderedString()
                + "' because there are already grantors for this instance.");
      }
      Set<KijiUser> newGrantor = Collections.singleton(user);
      putUsersWithPermission(KijiPermissions.Action.GRANT, newGrantor);
      grantAllWithoutLock(user);
    } finally {
      unlock();
    }
    LOG.info("Creator permissions on instance '{}' granted to user {}.",
        mInstanceUri,
        user.getName());
  }

  /** {@inheritDoc} */
  @Override
  public KijiPermissions getPermissions(KijiUser user) throws IOException {
    KijiPermissions result = KijiPermissions.emptyPermissions();

    for (KijiPermissions.Action action : KijiPermissions.Action.values()) {
      Set<KijiUser> usersWithAction = getUsersWithPermission(action);
      if (usersWithAction.contains(user)) {
        result = result.addAction(action);
      }
    }

    return result;
  }

  /** {@inheritDoc} */
  @Override
  public Set<KijiUser> listAllUsers() throws IOException {
    Set<KijiUser> allUsers = new HashSet<KijiUser>();
    for (KijiPermissions.Action action : KijiPermissions.Action.values()) {
      allUsers.addAll(getUsersWithPermission(action));
    }

    return allUsers;
  }

  /** {@inheritDoc} */
  @Override
  public void checkCurrentGrantAccess() throws IOException {
    KijiUser currentUser = KijiUser.getCurrentUser();
    if (!getPermissions(currentUser).allowsAction(KijiPermissions.Action.GRANT)) {
      throw new KijiAccessException("User " + currentUser.getName()
          + " does not have GRANT access for instance " + mInstanceUri.toString() + ".");
    }
  }

  /**
   * Grant action to a user, without locking the instance.  When using this method, one must lock
   * the instance before, and unlock it after.
   *
   * @param user User to grant action to.
   * @param action Action to grant to user.
   * @throws IOException on I/O error.
   */
  private void grantWithoutLock(KijiUser user, KijiPermissions.Action action) throws IOException {
    KijiPermissions currentPermissions = getPermissions(user);
    KijiPermissions newPermissions = currentPermissions.addAction(action);
    grantInstancePermissions(user, newPermissions);
  }

  /**
   * Grants all actions to a user, without locking the instance.  When using this method, one must
   * lock the instance before, and unlock it after.
   *
   * @param user User to grant all actions to.
   * @throws IOException on I/O error.
   */
  private void grantAllWithoutLock(KijiUser user)
      throws IOException {
    LOG.debug("Granting all permissions to user {} on instance '{}'.",
        user.getName(),
        mInstanceUri.toOrderedString());
    KijiPermissions newPermissions = getPermissions(user);
    for (KijiPermissions.Action action : KijiPermissions.Action.values()) {
      newPermissions = newPermissions.addAction(action);
    }
    grantInstancePermissions(user, newPermissions);
  }

  /**
   * Updates the permissions in the Kiji system table for a user on this Kiji instance.
   *
   * <p>Use {@link #grantInstancePermissions(KijiUser, KijiPermissions)}
   * or {@link #revokeInstancePermissions(KijiUser, KijiPermissions)}instead for updating the
   * permissions in HBase as well as in the Kiji system table.</p>
   *
   * @param user whose permissions to update.
   * @param permissions to be applied to this user.
   * @throws IOException If there is an I/O error.
   */
  private void updatePermissions(KijiUser user, KijiPermissions permissions)
      throws IOException {
    checkCurrentGrantAccess();
    for (KijiPermissions.Action action : KijiPermissions.Action.values()) {
      Set<KijiUser> permittedUsers = getUsersWithPermission(action);
      if (permissions.allowsAction(action)) {
        permittedUsers.add(user);
      } else {
        permittedUsers.remove(user);
      }
      putUsersWithPermission(action, permittedUsers);
    }
  }

  /**
   * Gets the users with permission 'action' in this instance, as recorded in the Kiji system
   * table.
   *
   * @param action specifying the permission to get the users of.
   * @return the list of users with that permission.
   * @throws IOException on I/O exception.
   */
  private Set<KijiUser> getUsersWithPermission(KijiPermissions.Action action) throws IOException {
    byte[] serialized = mSystemTable.getValue(action.getStringKey());
    if (null == serialized) {
      // If the key doesn't exist, no users have been put with that permission yet.
      return new HashSet<KijiUser>();
    } else {
      return KijiUser.deserializeKijiUsers(serialized);
    }
  }

  /**
   * Records a set of users as permitted to have action 'action', by recording them in the Kiji
   * system table.
   *
   * @param action to put the set of users into.
   * @param users to put to that permission.
   * @throws IOException on I/O exception.
   */
  private void putUsersWithPermission(
      KijiPermissions.Action action,
      Set<KijiUser> users)
      throws IOException {
    mSystemTable.putValue(action.getStringKey(), KijiUser.serializeKijiUsers(users));
  }

  /**
   * Changes the permissions of an instance, by granting the permissions on of all the Kiji meta
   * tables.
   *
   * <p>Permissions should be updated with #updatePermissions before calling this method.</p>
   *
   * @param user is the User to whom the permissions are being granted.
   * @param permissions is the new permissions granted to the user.
   * @throws IOException on I/O error.
   */
  private void grantInstancePermissions(
      KijiUser user,
      KijiPermissions permissions) throws IOException {
    LOG.info("Changing user permissions for user {} on instance {} to actions {}.",
        user,
        mInstanceUri,
        permissions.getActions());

    // Record the changes in the system table.
    updatePermissions(user, permissions);

    // Change permissions of Kiji system tables in HBase.
    KijiPermissions systemTablePermissions;
    // If this is GRANT permission, also add WRITE access to the permissions in the system table.
    if (permissions.allowsAction(KijiPermissions.Action.GRANT)) {
      systemTablePermissions =
          permissions.addAction(KijiPermissions.Action.WRITE);
    } else {
      systemTablePermissions = permissions;
    }
    grantHTablePermissions(user.getName(),
        KijiManagedHBaseTableName.getSystemTableName(mInstanceUri.getInstance()).toBytes(),
        systemTablePermissions.toHBaseActions());

    // Change permissions of the other Kiji meta tables.
    grantHTablePermissions(user.getName(),
        KijiManagedHBaseTableName.getMetaTableName(mInstanceUri.getInstance()).toBytes(),
        permissions.toHBaseActions());
    grantHTablePermissions(user.getName(),
        KijiManagedHBaseTableName.getSchemaIdTableName(mInstanceUri.getInstance()).toBytes(),
        permissions.toHBaseActions());
    grantHTablePermissions(user.getName(),
        KijiManagedHBaseTableName.getSchemaHashTableName(mInstanceUri.getInstance()).toBytes(),
        permissions.toHBaseActions());

    // Change permissions of all Kiji tables in this instance in HBase.
    Kiji kiji = Kiji.Factory.open(mInstanceUri);
    try {
      for (String kijiTableName : kiji.getTableNames()) {
        byte[] kijiHTableNameBytes =
            KijiManagedHBaseTableName.getKijiTableName(
                mInstanceUri.getInstance(),
                kijiTableName
            ).toBytes();
        grantHTablePermissions(user.getName(),
            kijiHTableNameBytes,
            permissions.toHBaseActions());
      }
    } finally {
      kiji.release();
    }
    LOG.debug("Permissions on instance {} successfully changed.", mInstanceUri);
  }

  /**
   * Changes the permissions of an instance, by revoking the permissions on of all the Kiji meta
   * tables.
   *
   * <p>Permissions should be updated with #updatePermissions before calling this method.</p>
   *
   * @param user User from whom the permissions are being revoked.
   * @param permissions Permissions to be revoked from the user.
   * @throws IOException on I/O error.
   */
  private void revokeInstancePermissions(
      KijiUser user,
      KijiPermissions permissions) throws IOException {
    // If GRANT permission is revoked, also remove WRITE access to the system table.
    KijiPermissions systemTablePermissions;
    if (permissions.allowsAction(KijiPermissions.Action.GRANT)) {
      systemTablePermissions =
          permissions.addAction(KijiPermissions.Action.WRITE);
    } else {
      systemTablePermissions = permissions;
    }
    revokeHTablePermissions(user.getName(),
        KijiManagedHBaseTableName.getSystemTableName(mInstanceUri.getInstance()).toBytes(),
        systemTablePermissions.toHBaseActions());

    // Change permissions of the other Kiji meta tables.
    revokeHTablePermissions(user.getName(),
        KijiManagedHBaseTableName.getMetaTableName(mInstanceUri.getInstance()).toBytes(),
        permissions.toHBaseActions());
    revokeHTablePermissions(user.getName(),
        KijiManagedHBaseTableName.getSchemaIdTableName(mInstanceUri.getInstance()).toBytes(),
        permissions.toHBaseActions());

    // Change permissions of all Kiji tables in this instance in HBase.
    for (String kijiTableName : mKiji.getTableNames()) {
      byte[] kijiHTableNameBytes =
          KijiManagedHBaseTableName.getKijiTableName(
              mInstanceUri.getInstance(),
              kijiTableName
          ).toBytes();
      revokeHTablePermissions(user.getName(),
          kijiHTableNameBytes,
          permissions.toHBaseActions());
    }
    LOG.debug("Permissions {} on instance '{}' successfully revoked from user {}.",
        permissions,
        mInstanceUri.toOrderedString(),
        user);
  }

  /**
   * Grants the actions to user on an HBase table.
   *
   * @param hUser HBase byte representation of the user whose permissions to change.
   * @param hTableName the HBase table to change permissions on.
   * @param hActions for the user on the table.
   * @throws IOException on I/O error, for example if security is not enabled.
   */
  private void grantHTablePermissions(
      String hUser,
      byte[] hTableName,
      Action[] hActions) throws IOException {
    LOG.debug("Changing user permissions for user {} on table {} to HBase Actions {}.",
        hUser,
        Bytes.toString(hTableName),
        Arrays.toString(hActions));
    LOG.debug("Disabling table {}.", Bytes.toString(hTableName));
    mAdmin.disableTable(hTableName);
    LOG.debug("Table {} disabled.", Bytes.toString(hTableName));

    // Grant the permissions.
    AccessControlProtos.AccessControlService.BlockingInterface protocol =
        AccessControlProtos.AccessControlService.newBlockingStub(
            mAccessControlTable.coprocessorService(HConstants.EMPTY_START_ROW)
        );
    try {
      ProtobufUtil.grant(protocol, hUser, TableName.valueOf(hTableName), null, null, hActions);
    } catch (Throwable throwable) {
      throw new KijiSecurityException("Encountered exception while granting access.",
          throwable);
    }

    LOG.debug("Enabling table {}.", Bytes.toString(hTableName));
    mAdmin.enableTable(hTableName);
    LOG.debug("Table {} enabled.", Bytes.toString(hTableName));
  }

  /**
   * Revokes the actions from user on an HBase table.
   *
   * @param hUser HBase byte representation of the user whose permissions to change.
   * @param hTableName the HBase table to change permissions on.
   * @param hActions for the user on the table.
   * @throws IOException on I/O error, for example if security is not enabled.
   */
  private void revokeHTablePermissions(
      String hUser,
      byte[] hTableName,
      Action[] hActions) throws IOException {
    LOG.debug("Revoking user permissions for user {} on table {} to HBase Actions {}.",
        hUser,
        Bytes.toString(hTableName),
        Arrays.toString(hActions));
    LOG.debug("Disabling table {}.", Bytes.toString(hTableName));
    mAdmin.disableTable(hTableName);
    LOG.debug("Table {} disabled.", Bytes.toString(hTableName));
    // Revoke the permissions.
    AccessControlProtos.AccessControlService.BlockingInterface protocol =
        AccessControlProtos.AccessControlService.newBlockingStub(
            mAccessControlTable.coprocessorService(HConstants.EMPTY_START_ROW)
        );
    try {
      ProtobufUtil.revoke(protocol, hUser, TableName.valueOf(hTableName), null, null, hActions);
    } catch (Throwable throwable) {
      throw new KijiSecurityException("Encountered exception while revoking access.",
          throwable);
    }
    LOG.debug("Enabling table {}.", Bytes.toString(hTableName));
    mAdmin.enableTable(hTableName);
    LOG.debug("Table {} enabled.", Bytes.toString(hTableName));
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mKiji.release();
    mLock.close();
    mZKClient.close();
  }
}
