package org.iu.engrcloudcomputing.memcached.keyvaluestore;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KeyValueStoreClient {

    public static void main(String[] args) {

        ExecutorService threadPool = Executors.newFixedThreadPool(50);


        threadPool.execute(new ParallelClient("master", "//Users//gkiran//Downloads/Walden-LifeInTheWot"));
//        for (int i = 0; i < 50; i++) {
//            String key = "India"+ i;
//            String value = "Country" + i;
//            threadPool.execute(new ParallelClient(key, value));
//        }
        threadPool.shutdown();
    }
}
