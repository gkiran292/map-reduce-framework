package org.iu.engrcloudcomputing.memcached.keyvaluestore.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.iu.engrcloudcomputing.memcached.keyvaluestore.ds.UniqueLinkedBlockingQueue;
import org.iu.engrcloudcomputing.memcached.keyvaluestore.impl.KeyValueStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KeyValueStoreServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getSimpleName());
    private static UniqueLinkedBlockingQueue<String> blockingQueue = new UniqueLinkedBlockingQueue<>();
    private static ConcurrentMap<String, String> concurrentMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {

        //init
        readAndWriteFromFile(args[0]);
        int port = Integer.parseInt(args[1]);
        Server server = ServerBuilder.forPort(port)
                .addService(new KeyValueStoreService(concurrentMap, blockingQueue, args[0])).build();
        server.start();

        LOGGER.info("Key Value Store Server Started at {}", server.getPort());
        server.awaitTermination();

    }

    private static void readAndWriteFromFile(String filePath) throws IOException, URISyntaxException {

        File file = new File(new URI(filePath));
        if (!file.exists()) {
            if (!file.createNewFile()) {
                LOGGER.error("Couldn't create a new file {}", filePath);
                throw new IOException("Couldn't create a new file tmp.txt");
            }
        }

        BufferedReader br = new BufferedReader(new FileReader(file.getAbsoluteFile()));

        String line;
        while ((line = br.readLine()) != null) {
            String[] splitInput = line.split("~");

            try {
                concurrentMap.put(splitInput[0], splitInput[1]);
            } catch (Exception ignored) { }

        }

        br.close();

        BufferedWriter bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile(), false));
        concurrentMap.forEach((key, value) -> {
            try {
                bw.write(key + "~" + value + "\n");
            } catch (IOException e) {
                LOGGER.error("Couldn't write to file {}", filePath);
            }
        });

        bw.close();
    }
}
