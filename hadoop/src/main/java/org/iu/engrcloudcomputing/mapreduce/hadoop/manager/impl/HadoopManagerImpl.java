package org.iu.engrcloudcomputing.mapreduce.hadoop.manager.impl;

import com.google.protobuf.ProtocolStringList;
import io.grpc.ManagedChannel;
import org.iu.engrcloudcomputing.mapreduce.hadoop.autogenerated.BeginMapReduceGrpc;
import org.iu.engrcloudcomputing.mapreduce.hadoop.autogenerated.Master;
import org.iu.engrcloudcomputing.mapreduce.hadoop.autogenerated.ShutDownMasterGrpc;
import org.iu.engrcloudcomputing.mapreduce.hadoop.exception.ClusterShutDownException;
import org.iu.engrcloudcomputing.mapreduce.hadoop.exception.MapReduceFailureException;
import org.iu.engrcloudcomputing.mapreduce.hadoop.helper.JavaProcessBuilder;
import org.iu.engrcloudcomputing.mapreduce.hadoop.manager.spi.HadoopManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessResult;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class HadoopManagerImpl implements HadoopManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getSimpleName());

    @Override
    public Future<ProcessResult> initiateCluster(String masterJar, String workingFolder, String ipAddress, int port) throws IOException {

        LOGGER.debug("Master Jar: {}, ipAddress: {}, Port: {}", masterJar, ipAddress, port);
        JavaProcessBuilder javaProcessBuilder = new JavaProcessBuilder();
        javaProcessBuilder.addArgument(String.valueOf(port));
        javaProcessBuilder.addArgument(workingFolder);
        javaProcessBuilder.setJarPath(masterJar);
        return javaProcessBuilder.asyncStartProcess();
    }

    @Override
    public List<String> runMapReduce(ManagedChannel channel, String kvStoreIpAddress, int kvStorePort, String masterIpAddress,
                                     int masterPort, int mapperCount, int reducerCount, String mapperJar, String reducerJar,
                                     String initialKey) {

        BeginMapReduceGrpc.BeginMapReduceBlockingStub blockingStub = BeginMapReduceGrpc.newBlockingStub(channel);

        Master.MapReduceResponse mapReduceResponse = blockingStub.mapReduce(Master.MapReduceParams.newBuilder().setInitialKey(initialKey)
                .setMapperJar(mapperJar)
                .setMappers(mapperCount)
                .setReducerJar(reducerJar)
                .setReducers(reducerCount)
                .setKvStoreIpAddress(kvStoreIpAddress)
                .setMasterIpAddress(masterIpAddress)
                .setMasterPort(masterPort)
                .setKvStorePort(kvStorePort).build());

        Master.Message message = mapReduceResponse.getMessage();

        int responseCode = message.getResponseCode();
        if (responseCode != 200) {
            LOGGER.error("MapReduce task failed with status: {}", responseCode);
            throw new MapReduceFailureException("Map Reduce Task failed");
        }

        ProtocolStringList keyList = mapReduceResponse.getKeys().getKeyList();
        LOGGER.info("Map Reduce Task was successful ");

        return new ArrayList<>(keyList);
    }

    @Override
    public boolean destroyCluster(String ipAddress, int port, ManagedChannel channel) {

        LOGGER.info("Shutting down the master running on the ipAddress: {}, port: {}", ipAddress, port);
        ShutDownMasterGrpc.ShutDownMasterBlockingStub blockingStub = ShutDownMasterGrpc.newBlockingStub(channel);

        if (!channel.isShutdown()) {
            Master.Message message = blockingStub.shutDown(Master.Empty.newBuilder().build());

            if (message.getResponseCode() != 200) {
                LOGGER.error("Not able to shutdown the server, ipAddress: {}, port: {}", ipAddress, port);
                throw new ClusterShutDownException("Unable to shutdown the cluster");
            }
        }

        return true;
    }
}