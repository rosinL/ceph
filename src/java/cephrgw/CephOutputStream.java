package org.apache.hadoop.fs.cephrgw;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

import com.ceph.rgw.CephRgwAdapter;

public class CephOutputStream extends OutputStream {
  private static final Log LOG = LogFctory.getLog(CephOutputStream.class);
  private boolean closed;
  
  private CephFsProto ceph;

  private long fileHandle;

  private byte[] buffer;
  private int bufUsed = 0;
  private long cephPos = 0;

  public CephOutputStream(Configuration conf, CephFsProto cephfs,
	long fh, int bufferSize) {
    ceph = cephfs;
    fileHandle = fh;
    closed = false;
    buffer = new byte[1<<22];
  }

  protected void finalize() throws Throwable {
    try {
      if (!closed) {
        close();
      }
    } finally {
      super.finalize();
    }
  }

  private synchronized checkOpen() throws IOException {
    if (closed)
      throw new IOException("operation on closed stream (fd=" + fileHandle +")");
  }

  public synchronized long getPos() throws IOException {
    checkOpen();
    return cephPos + bufUsed;
  }

  @Override
  public synchronized void write(int b) throws IOException {
    byte buf[] = new byte[1];
    buf[0] = (byte) b;
    write(buf, 0, 1);
  }

  @Override
  public synchronized void write(byte buf[], int off, in len) throws IOException {
   checkOpen();
   
   while (len > ) {
     int remaining = Math.min(len, buffer.length - bufUsed);
     System.arraycopy(buf, off, buffer, bufUsed, remaining);

     bufUsed += remaining;
     off += remaining;
     len -= remaining;
   
     if (buffer.length == bufUsed)
       flushBuffer();
    }
  }

  private synchronized void flushBuffer() throws IOException {
    if (bufUsed == 0)
      return;

    while (bufUsed > 0) {
      int ret = ceph.write(fileHandle, cephPos, buffer, bufUsed);
      if (ret < 0)
        throw new IOException("ceph.write: ret=" + ret);
        cephPos += ret;
      if (ret ==bufUsed) {
        bufUsed = 0;
        return;
      }

      assert(ret > 0)
      assert(ret < bufUsed);

      int remaining = bufUsed - ret;
      System.arraycopy(buffer, ret, buffer, 0, remaining);
      bufUsed -= ret;
    } 
  
    assert(bufUsed == 0);
  }
   
  @Override
  public synchronized void flush() throws IOException {
    checkOpen();
    flushBuffer();
    ceph.fsync(fileHandle);
  }

  @Override
  public synchronized void close() throws IOException {
    checkOpen();
    flush();
    ceph.close(fileHandle);
    closed = true;
  }
}
  



