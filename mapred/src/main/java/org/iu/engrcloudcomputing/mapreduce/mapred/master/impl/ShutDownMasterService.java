package org.iu.engrcloudcomputing.mapreduce.mapred.master.impl;

import io.grpc.stub.StreamObserver;
import org.iu.engrcloudcomputing.mapreduce.mapred.autogenerated.Master;
import org.iu.engrcloudcomputing.mapreduce.mapred.autogenerated.ShutDownMasterGrpc;

import java.util.concurrent.CountDownLatch;

public class ShutDownMasterService extends ShutDownMasterGrpc.ShutDownMasterImplBase {

    private CountDownLatch countDownLatch;

    public ShutDownMasterService(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void shutDown(Master.Empty request, StreamObserver<Master.Message> responseObserver) {

        try {
            countDownLatch.countDown();
            responseObserver.onNext(Master.Message.newBuilder().setResponseCode(200).setResponseMessage("OK").build());
        } catch (Exception e) {
            responseObserver.onNext(Master.Message.newBuilder().setResponseCode(500).setResponseMessage("ERROR").build());
        }
        finally {
            responseObserver.onCompleted();
        }
    }
}
