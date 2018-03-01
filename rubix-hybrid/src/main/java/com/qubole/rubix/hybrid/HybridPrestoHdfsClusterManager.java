package com.qubole.rubix.hybrid;

import com.qubole.rubix.hadoop2.Hadoop2ClusterManager;
import com.qubole.rubix.presto.PrestoClusterManager;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class HybridPrestoHdfsClusterManager extends ClusterManager {

  private Hadoop2ClusterManager   hdfsClusterManager;
  private PrestoClusterManager  prestoClusterManager;

  public HybridPrestoHdfsClusterManager() {
  }

  public HybridPrestoHdfsClusterManager(Hadoop2ClusterManager hdfsClusterManager,
                                        PrestoClusterManager prestoClusterManager) {
    this.hdfsClusterManager = hdfsClusterManager;
    this.prestoClusterManager = prestoClusterManager;
  }

  @Override
  public void initialize(Configuration conf) {

    if (hdfsClusterManager == null) {
      hdfsClusterManager = new Hadoop2ClusterManager();
      hdfsClusterManager.initialize(conf);
    }

    if (prestoClusterManager == null) {
      prestoClusterManager = new PrestoClusterManager();
      prestoClusterManager.initialize(conf);
    }

    super.initialize(conf);
  }

  @Override
  public ClusterType getClusterType() {
    return ClusterType.HYBRID_PRESTO_HDFS_CLUSTER_MANAGER;
  }

  @Override
  public boolean isMaster() throws ExecutionException {
    return prestoClusterManager.isMaster();
  }

  @Override
  public List<String> getNodes() {
    return prestoClusterManager.getNodes();
  }

}
