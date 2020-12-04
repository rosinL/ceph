
package org.apache.hadoop.fs.cephrgw;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.InetAddress;
import java.util.EnumSet;
import java.lang.Math;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.fs.FsStatus;

import com.ceph.rgw.CephFileAlreadyExistsException;
import com.ceph.rgw.CephRgwAdapter;
import com.ceph.rgw.CephStat;
import com.ceph.rgw.CephStatVFS;


public class CephRgwFileSystem extends FileSystem {
  private static final Log LOG = LogFactory.getLog(CephRgwFileSystem.class);
  private URI uri;

  private Path workingDir;
  private CephFsProto ceph = null;

  public CephRgwFileSystem() {
  }

  public CephRgwFileSystem(Configuration conf) {
    setConf(conf);
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    LOG.error("non abs path " + path + " workingdir " + workingDir);
    return new Path(workingDir, path);
  }

  public URI getUri() {
    return uri;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    if (ceph == null) {
      ceph = new CephTalker(conf, LOG);
    }
    ceph.initialize(uri, conf);
    setConf(conf);
    try {
      this.uri = new URI(uri.getScheme(), uri.getAuthority(), "", "");
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    this.workingDir = getHomeDirectory();
  }

  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    path = makeAbsolute(path);

    long fd = ceph.open(path, CephRgwAdapter.O_RDONLY, 0);

    CephStat stat = new CephStat();
    ceph.lstat(path, stat);

    CephInputStream istream = new CephInputStream(getConf(), ceph, fd, stat, size, bufferSize);
    return new FSDataInputStream(istream);
  }

  @Override
  public void close() throws IOException {
    super.close();
    ceph.shutdown();
  }

  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
    path =makeAbsolute(path);

    if (progress != null) {
      progress.progress();
    }

    long fd = ceph.open(path, CephRgwAdapter.O_WRONLY|CephRgwAdapter, O_APPEND, 0);

    if (progress != null) {
      progress.progress();
    }

    CephOutputStream ostream = new CephOutputStream(getConf(), ceph, fd, buffersize);
    return new FSDataOutputStream(ostream, statistics)
  }

  public Path getWorkingDirectory()
    return workingDir();
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    workingDir = makeAbsolute(dir);
  }

  @Override
  public boolean mkdirs(Path path, FsPermission perms) throws IOException {
    path = makeAbsolute(path);

    boolean result = false;
    try {
      ceph.mkdirs(path, (int) perms.toShort());
      result = true;
    } catch (CephFileAlreadyExistsException e) {
      result = true;
    }

    return result;
  }


  @Override
  public boolean mkdirs(Path f) throws IOException {
    return mkdirs(f, FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(getConf())));
  } 
 
  public FileStatus getFileStatus(Path path) throws IOException {
    path = makeAbsolute(path);

    String pathStr = Path.getPathWithoutSchemeAndAuthority(path).toString();
    LOG.error("lstat path " + path + " ceph path " + pathStr);
    CephStat stat = new CephStat();
    ceph.lstat(path, stat);

    FileStatus = new FileStatus(stat.size, stat.isDir(),
 	ceph.getDefaultReplication(), stat.blksize, stat.m_time,
	stat.a_time(), new FsPermission((short) stat.mode),
	System.getProperty("user.name"), null, path.makeQualified(this));

    reutn status;
  }

  public FileStatus[] listStatus(Path path) throws IOException {
    path = makeAbsolute(path);

    if (isFile(path))
      return new FileStatus[] { getFileStatus(path) };

    String[] dirlist = ceph.listdir(path);
    if (dirlist != null) {
      FileStatus[] status = new FileStatus[dirlist.length];
      for (int i = 0; i < status.length; i++) {
        LOG.error("list path " + path + " lsdir " + dirlist[i]);
        status[i] = getFileStatus(new Path(path, dirlist[i]));
      }
      reutn status;
    }
    else {
      throw new FileNotFoundException("File " + path + " does not exist.");
    }
  }

  @Overrid
  public void setPermission(Path path, FsPermission permission) throws IOException {
    path = makeAbsolute(path);

    CephStat stat = new CephStat();
    stat.mode = permission.toShort();
    ceph.setattr(path, stat, CephRgwAdapter.SETATTR_MODE);
  }

  @Overrid
  public void setTimes(Path path, long mtime, long atime) throws IOException {
    path = makeabsolute(path);
    
    CephStat stat = new CephStat();
    int mask = 0;

    if (mtime != -1)
      mask |= CephRgwAdapter.SETATTR_MTIME; 
      stat.m_time = mtime;
    }

    if (atime != -1)
      mask |= CephRgwAdapter.SETATTR_ATIME;
      stat.a_time = atime;
    }

    ceph.setattr(path, stat, mask);
  }

  public FSDataOutputStream create(Path path, FsPermission permission,
	boolean overwrite, int bufferSize, short replication, long blockSize,
	Progressable progress) throws IOException {
    path = makeAbsolute(path);

    boolean exists = exists(path);

    if (progress != null) {
      progress.progress();
    }

    int flags = CephRgwAdapter.O_WRONLY | CephRgwAdapter.O_CREATE;

    if (exists) {
      if (overwrite)
        flags != CephRgwAdapter.O_TRUNC;
      else
        throw new FileAlreadyExistsException():
    } else {
      Path parent = path.getParent();
      if (parent != null)
        if (!mkdirs(parent))
          throw new IOexception("mkdirs failed for " + parent.toString());
    }

    if (progress != null) {
      progress.progress();
    }

    if (blockSize > Integer.MAX_VALUE) {
      blockSize = Interger.MAX_VALUE;
      LOG.info("blockSize too large. Rounding down to " + blockSize);
    }

    if (blockSize <= 0)
      throw new IllegalArgumentException("Invalid block size: " + blockSize);

    long fd = ceph.open(path, flags, (int)permission.toShort());

    if (progress != null)
      progress.progress();
    }

    OutPutStream ostream = new CephOutputStream(getConf(), ceph, fd, bufferSize);
    return new FSDataOutputStream(ostream, statistics);
  }

  public FSDataOutputStream createNonRecursive(Path path, FsPermission permission, 
	boolean overwrite,
	int buffersize, short replication, long blockSize, 
	Progressable progress) throws IOException {

    path = makeAbsolute(path);

    Path parent = path.getParent();

   if (parent != null)
     CephStat = new CephStat();
     ceph.lstat(parent, stat);
     if (stat.isFile())
       throw new FileAlreadyExistsException(parent.toString());
     }

   return this.create(path, permission, overwrite,
	bufferSize, replication, blockSize, progress);
  }

  @Override
  public boolean rename(Path src Path dst) throws IOException {
    boolean ret = false;
    src = makeAbsolute(src);
    dst = makeAbsolute(dst);

    try {
      ret = ceph.rename(src, dst);
    } catch (FileNotFoundException e) {
      throw e;
    } catch (Exception e) {
      return ret;
    }

    return ret;
  }

  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, false);
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    path = makeAbsolute(path);

    FileStatus status;
    try {
      status = getFileStatus(path);
    } catch (FileNotFoundException e) {
      return false;
    }

    if (status.isFile()) {
      ceph.unlink(path);
      return true;
    }

    FileStatus[] dirlist = listStatus(path);
    if (dirlist == null)
      return false;

    if (!recursive && dirlist.length > 0)
      throw new IOException("Directory " + path.toString() + "is not empty.");

    for (FileStatus fs : dirlist) {
      if (!delete(fs.getPath(), recursive))
        return false;
    }

    ceph.unlink(path);
    return true;
  }

  @Override
  public short getDefaultReplication() {
    return ceph.getDefaultReplication();
  }
  
  @Override
  public long getDefaultBlockSize() {
    return getConf().getLong(
	CephConfigKeys.CEPH_RGW_BLOCKSIZE_KEY,
	CephConfigKeys.CEPH_RGW_BLOCKSIZE_DEFAULT);
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    CephStatVFS stat = new CephStatVFS();
    ceph.statfs(p, stat);
    
    FsStatus status = new FsStatus(stat.bsize * stat.blocks,
 	stat.bsize * (stat.blocks - stat.bavail),
	stat.bsize * stat.bavail);
    return status;
  }
}
 




