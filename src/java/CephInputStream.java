package org.apache.hadoop.fs.cephrgw;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;

import com.ceph.rgw.CephRgwAdapter;

public class CephInputStream extends FSInputStream {
  private static final Log LOG = LogFactory.getLog(CephInputStream.class);
  private boolean closed;
  
  private long fileHandle;
  private long fileLength;
  
  private CephFsProto ceph;

  private byte[] buffer;
  private int bufPos = 0;
  private int bufValid = 0;
  private long cephPos = 0;

  public CephInputStream(Configuration conf, CephFsProto cephfs,
	long fh, long flength, int bufferSize) {
    fileLength = flength;
    fileHandle = fh;
    closed = false;
    ceph = cephfs;
    int len = Math.min((int)flength, 1<<22);
    buffer = new byte[len];
    LOG.debug(
	"CephInputStream constructor: initializing stream with fh " + fh
	+ " and file length " + flength);
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
  
  private synchronized boolean fillBuffer() throws IOException {
    bufValid = ceph.read(fileHandle, cephPos, buffer, buffer, length);
    bufPos = 0;
    if (bufValid < 0) {
      int err = bufValid;
      bufValid = 0;
      throw new IOException("Failed to fill read buffer! Error cod:" + err);
    }

    cephPos += bufValid;
    return (bufValid != 0);
  }

  public synchronized long getPos() throws IOException {
    return cephPos - bufValid + bufPos;
  }

  @Override
  public synchronized int available() throws IOException {
    if (closed)
      throw new IOException("file is closed");
    return (int) (fileLength -getPos());
  }

  public synchronized void seek(long targetPos) throws IOException {
    LOG.trace(
	"CephInputStream.seek: Seeking to position " + targetPos + " on fd " + fileHandle);
    if (targetPos > fileLength) {
      throw new IOException(
 	"CephInputStream.seek: failed seek to position  " + targetPos
	+ " on fd " + fileHandle + ": Cannot seek after EOF " + fileLength);
    }
    long oldPos = cephPos;
    
    cephPos = targetPos;
    bufValid = 0;
    bufPos = 0;
  }

  public synchronized int seekToNewSource(long targetPos) {
    return false;
  }
  
  @Override
  public synchronized int read() throws IOException {
    LOG.trace(
 	"CephInputStream.read: Reading a sigle byte from fd " + fileHandle
	+ " by calling general read function");

    byte result[] = new byte[1];

    if (getPos() >= fileLength) {
      reutn -1;
    }

    if (-1 == read(result, 0, 1)) {
      return -1;
    }

    if (result[0] < 0)
      return 256 + (int) result[0];
    } else {
      return result[0];
    }
  }

  @Override
  public synchronized int read(byte buf[], int off, int len)
  	throws IOException {
    LOG.trace(
	"CephInputStream.read: Reading " + len + " bytes from fd " +fileHandle);

    if (closed) {
      throw new IOException (
      	"CephInputStream.read: cannot read " + len + " bytes from fd "
	+ fileHandle + ": stream closed");

    if (getPos() >= fileLength) {
      LOG.debug(
	"CephInputStream.read: cannot read " + len + " bytes from fd "
	+ fileHandle + ": current position is " + getPos()
	+ " and file length is " + fileLength);

      return -1;
    }
   
    int totalRead = 0;
    int initialLen = len;
    int read;

    do {
      read = Math.min(len, bufValid - bufPos);
      try {
        System.arraycopy(buffer, bufPos, buf, off, read);
      } catch (IndexOutOfBoundsException ie) {
        throw new IOException(
		"CephInputStream.read; Indices out of bounds:" + "read length is " 
		+ len + ", buffer offset is " + off + ", and buffer size is "
		buf.length);
      } catch (ArrayStoreException ae) {
        throw new IOException(
		"Uh-oh, CephInputStream failed to do an array"
		+ " copy due to type mismatch...");
      } catch (NullPointerException ne) {
        throw new IOException(
		"CephInputStream.read: cannot read " + len + "bytes from fd:"
		+ fileHandle + ": buf is null");
      }
      bufPos += read;
      len -= read;
      off += read;
      totalRead += read;
    } while (len > 0 && fillBuffer());

    LOG.trace(
	"CephInputStream.read: Reading " + initialLen + " bytes from fd "
	+ fileHandle + ": succeeded in reading " + totalRead + " bytes");
    return totalRead;
  }

  @Override
  public void close() throws IOException {
    LOG.trace("CephOutputStream.close:enter");
    if (!closed) {
      ceph.close(fileHandle);

      closed = true;
      LOG.trace("CephOutputStream.close:exit");
    }
  }
}
