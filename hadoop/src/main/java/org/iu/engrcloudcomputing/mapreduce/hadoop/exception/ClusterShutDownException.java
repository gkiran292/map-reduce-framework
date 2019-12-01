package org.iu.engrcloudcomputing.mapreduce.hadoop.exception;

public class ClusterShutDownException extends RuntimeException {

    public ClusterShutDownException(String message) {
        super(message);
    }
}
