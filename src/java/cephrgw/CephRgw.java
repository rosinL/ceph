package org.apache.hadoop.fs.cephrgw

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystm;
import org.apache.hadoop.fs.AbstractFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CephRgw extends DelegateToFileSystem {

  CephRgw(final URI theUri, final Configuration conf) throws IOException,
	URISyntaxException {
    super(theUri, new CephRgwFileSystem(conf), conf, "cephrgw", true);
  }
}
