package org.iu.engrcloudcomputing.mapreduce.hadoop.manager.spi;

import io.grpc.ManagedChannel;
import org.zeroturnaround.exec.ProcessResult;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;

public interface HadoopManager {

    Future<ProcessResult> initiateCluster(String masterJar, String workingFolder, String ipAddress, int port) throws IOException;

    List<String> runMapReduce(ManagedChannel channel, String kvStoreIpAddress, int kvStorePort, String masterIpAddress,
                              int masterPort, int mapperCount, int reducerCount, String mapperJar, String reducerJar,
                              String initialKey);

    boolean destroyCluster(String ipAddress, int port, ManagedChannel channel);
}
