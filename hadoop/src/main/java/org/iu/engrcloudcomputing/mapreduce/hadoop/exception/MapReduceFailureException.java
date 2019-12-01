package org.iu.engrcloudcomputing.mapreduce.hadoop.exception;

public class MapReduceFailureException extends RuntimeException {

    private static final long serialVersionUID = 1141714651063670250L;

    public MapReduceFailureException(String message) {
        super(message);
    }
}

