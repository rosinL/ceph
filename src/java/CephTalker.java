package org.apache.hadop.fs.cephrgw;

import java.io.IOException;
import java.net.URI;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.net.InetAddress;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.lang.StringUtils;

import com.ceph.rgw.CephRgwAdapter;
import com.ceph.rgw.CephStat;
import com.ceph.rgw.CephStatVFS;
import com.ceph.rgw.CephFileAlreadyExistsException;

class CephTalker extends CephFsProto {

  private CephRgwAdapter rgwAdapter;
  private short defaultReplication;

  public CephTalker(Configuration conf, Log log) {
    rgwAdapter = null;
  }

  private String pathString(Path path) {
    if (null == path) {
      return "/";	
    }
    return path.toUri().getPath().substring(1);
  }

  void initialize(URI uri, Configuration conf) throws IOException {
    String args = conf.get(
      CephConfigKeys.CEPH_RGW_ARGS_KEY,
      CephConfigKeys.CEPH_RGW_ARGS_DEFAULT);
    rgaAdapter = new CephRgwAdapter(args);

    String userId = conf.get(
      CephConfigKeys.CEPH_RGW_USERID_KEY,
      CephConfigKeys.CEPH_RGW_USERID_DEFAULT);
      
    String accessKey = conf.get(
      CephConfigKeys.CEPH_RGW_ACCESS_KEY_KEY,
      CephConfigKeys.CEPH_RGW_ACCESS_KEY_DEFAULT);
    String secretKey = conf.get(
      CephConfigKeys.CEPH_RGW_SECRET_KEY_KEY,
      CephConfigKeys.CEPH_RGW_SECRET_KEY_DEFAULT);

    defaultReplication = (short)conf.getInt(
      CephConfigKeys.CEPH_REPLICATION_KEY,
      CephConfigKeys.CEPH_REPLICATION_DEFAULT);

    rgwAdapter.mount(userId, accessKey, secretKey, uri.getAuthority());
  }

  long __open(Path path, int flags, int mode) throws IOException {
    return rgwAdapter.open(pathString(path), flags, mode);
  }

  long open(Path path, int flags, int mode) throws IOException {
    long fd = __open(path, flags, mode);
    CephStat stat = new CephStat();
    lstat(path, stat);
    if (stat.isDir()) {
      rgwAdapter.close(fd);
      throw new FileNotFoundException();
    }
    return fd;
  }

  void lstat(Path path, CephStat stat) throws IOException {
    try {
      rgwAdapter.lstat(pathString(path), path.getName(), stat);
    } catch (FileNotFoundException e) {
      throw new FileNotFoundException();
    }
  }

  void statfs(Path path, CephStatVFS stat) throws IOException {
    try {
      rgwAdapter.statfs(pathString(path), stat);
    } catch (FileNotFoundException e) {
      throw new FileNotFoundException();
    }
  }

  void unlink(Path path) throws IOException {
    rgwAdapter.unlink(pathString(path));
  }

  boolean rename(Path from, Path to) throws IOException {
    try {
      int ret = rewAdapter.rename(pathString(from.getParent()), from.getName(), 
				pathString(to.getParent()), to.getName());
      if (ret == 0)
        return true;
    } catch (FileNotFoundException e) {
      throw new FileNotFoundException();
    }
    return false;
  }

  String[] listdir(Path path)  throws IOException {
    CephStat stat = new CephStat();
    try {
      rgwAdapter.lstat(pathString(path.getParent()), path.getName(), stat);
    } catch (FileNotFoundException e) {
      return null;
    }
    if (!stat.isDir())
      return null;
    return rgwAdapter.listdir(pathString(path));
  }

  void mkdirs(Path path, int mode) throws IOException {
    rgwAdapter.mkdirs(pathString(path.getParent(), path.getName(), mode);
  }

  void close(long fd) throws IOException {
    rgwAdapter.close(fd);
  }

  void shutdown() throws IOException {
    if (null != rgwAdapter)
      rgwAdapter.unmount();
    rgwAdapter = null;
  }

  short getDefaultReplication()
    return defaultReplication;
  }

  void setattr(Path path, CephStat stat, int mask) throws IOException {
    rgwAdapter.setattr(pathString(path), stat, mask);
  }

  void fsync(long fd) throws IOException {
    rgwAdapter.fsync(fd, false);
  }

  int write(long fd, long offset, byte[] buf, long size) throws IOException {
    return (int)rgwAdapter.write(fd, offset, buf, size);
  }
  
  int read(long fd, long offset, byte[] buf, long size) throws IOException {
    return (int)rgwAdapter.read(fd, offset, buf, size);
  }

}
  
  
   















