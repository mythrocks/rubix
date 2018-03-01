package com.qubole.rubix.hybrid;

import com.qubole.rubix.core.CachingFileSystem;
import com.qubole.rubix.hadoop2.CachingDistributedFileSystem;
import com.qubole.rubix.presto.CachingPrestoS3FileSystem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;


public class HybridPrestoHadoopFileSystem extends CachingFileSystem<DistributedFileSystem> {

  private static final Log LOG = LogFactory.getLog(HybridPrestoHadoopFileSystem.class);

  private CachingDistributedFileSystem hdfsFs = new CachingDistributedFileSystem();
  private CachingPrestoS3FileSystem prestoFS = new CachingPrestoS3FileSystem();

  private HybridPrestoHdfsClusterManager hybridPrestoHdfsClusterManager;

  @Override
  public String getScheme() {
    return hdfsFs.getScheme();
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {

    LOG.info("CALEB: HybridPrestoHadoopFileSystem::initialize()! With uri: " + uri);
    hdfsFs.initialize(uri, conf);
    prestoFS.initialize(uri, conf);

    if (hybridPrestoHdfsClusterManager == null) {
      initializeClusterManager(conf);
    }
    super.initialize(uri, conf);
  }

  synchronized void initializeClusterManager(Configuration conf) {
    if (hybridPrestoHdfsClusterManager == null) {
      hybridPrestoHdfsClusterManager = new HybridPrestoHdfsClusterManager(hdfsFs.getClusterManager(), prestoFS.getClusterManager());
      hybridPrestoHdfsClusterManager.initialize(conf);
    }
    setClusterManager(hybridPrestoHdfsClusterManager);
  }

}

/*
public class HybridPrestoHadoopFileSystem extends CachingDistributedFileSystem {

  private static final Log LOG = LogFactory.getLog(HybridPrestoHadoopFileSystem.class);
  private HybridPrestoHdfsClusterManager clusterManager;

  @Override
  public String getScheme() {
    return null;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    LOG.info("CALEB: HybridPrestoHadoopFileSystem::initialize()! With uri: " + uri);
    if (clusterManager == null) {
      initializeClusterManager(conf);
    }
    super.initialize(uri, conf);
  }

  protected synchronized void initializeClusterManager(Configuration conf) {
    LOG.info("CALEB: HybridPrestoHadoopFileSystem::initializeClusterManager()! ");
    if (clusterManager == null) {
      clusterManager = new HybridPrestoHdfsClusterManager();
      clusterManager.initialize(conf);
    }
  }
}
*/
