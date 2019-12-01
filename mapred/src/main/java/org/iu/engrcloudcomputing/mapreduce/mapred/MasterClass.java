package org.iu.engrcloudcomputing.mapreduce.mapred;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.iu.engrcloudcomputing.mapreduce.mapred.master.impl.InitiateMapReduceService;
import org.iu.engrcloudcomputing.mapreduce.mapred.master.impl.MapperAckService;
import org.iu.engrcloudcomputing.mapreduce.mapred.master.impl.ReducerAckService;
import org.iu.engrcloudcomputing.mapreduce.mapred.master.impl.ShutDownMasterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

public class MasterClass {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getSimpleName());

    public static void main(String[] args) throws IOException, InterruptedException {

        ConcurrentMap<String, String> mapperConcurrentMap = new ConcurrentHashMap<>();
        ConcurrentMap<String, String> reducerConcurrentMap = new ConcurrentHashMap<>();
        CountDownLatch mapReduceTaskLatch = new CountDownLatch(1);
        CountDownLatch serverStatusLatch = new CountDownLatch(1);

        //Register mapperAckService and reducerAckService

        LOGGER.info("Master is initializing...");
        Server server = ServerBuilder.forPort(Integer.parseInt(args[0]))
                .addService(new MapperAckService(mapperConcurrentMap))
                .addService(new ReducerAckService(reducerConcurrentMap))
                .addService(new InitiateMapReduceService(mapperConcurrentMap, reducerConcurrentMap, mapReduceTaskLatch, args[1]))
                .addService(new ShutDownMasterService(serverStatusLatch)).build();
        server.start();
        LOGGER.info("Master is initialized");

        serverStatusLatch.await();
        Thread.sleep(100);
        server.shutdownNow();
    }
}