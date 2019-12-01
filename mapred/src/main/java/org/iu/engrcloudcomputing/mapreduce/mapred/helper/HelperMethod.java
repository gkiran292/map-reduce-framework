package org.iu.engrcloudcomputing.mapreduce.mapred.helper;

import com.google.protobuf.ProtocolStringList;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ConcurrentMap;

public class HelperMethod {

    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getSimpleName());

    public static void storeKeys(ConcurrentMap<String, String> concurrentMap, ProtocolStringList keyList) {

        for (String key : keyList) {
            key = StringUtils.trim(key);
            String[] split = key.split("_");
            try {
                concurrentMap.put(split[0] + "_" + split[1], split[0] + "_" + split[1]);
                LOGGER.debug("Mapper id: {}, Key: {}", split[0], split[1]);
            } catch (ArrayIndexOutOfBoundsException e) {
                LOGGER.warn("No Key present (improve mapper... unnecessarily consuming bandwidth) mapperId: {}", split[0]);
            }
        }
    }
}
