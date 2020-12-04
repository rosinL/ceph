package org.apache.hadoop.fs.cephrgw;

import org.apache.hadoop.fs.CommonConfigurationKeys;


public class CephConfigKeys extends CommonConfigurationKeys {
  public static final String CEPH_RGW_ARGS_KEY = "fs.ceph.rgw.args";
  public static final String CEPH_RGW_ARGS_DEFAULT = "--name=client.admin";

  public static final String CEPH_RGW_BLOCKSIZE_KEY = "fs.ceph.rgw.blocksize";
  public static final long   CEPH_RGW_BLOCKSIZE_DEFAULT = 64*1024*1024;

  public static final String CEPH_REPLICATION_KEY = "fs.ceph.replication";
  public static final short  CEPH_REPLICATION_DEFAULT = 3;

  public static final String CEPH_RGW_USERID_KEY = "fs.ceph.rgw.userid";
  public static final String CEPH_RGW_USERID_DEFAULT = null;

  public static final String CEPH_RGW_ACCESS_KEY_KEY = "fs.ceph.rgw.access.key";
  public static final String CEPH_RGW_ACCESS_KEY_DEFAULT = null;

  public static final String CEPH_RGW_SECRET_KEY_KEY = "fs.ceph.rgw.secret.key";
  public static final String CEPH_RGW_SECRET_KEY_DEFAULT = null
}

