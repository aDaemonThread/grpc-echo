package org.akram.grpc.echo.service;

import com.example.EchoMessage;
import com.example.EchoServiceGrpc;
import net.devh.boot.grpc.server.service.GrpcService;

import io.grpc.stub.StreamObserver;

@GrpcService
public class EchoServiceImpl extends EchoServiceGrpc.EchoServiceImplBase {

    @Override
    public void echoUnary(EchoMessage request, StreamObserver<EchoMessage> responseObserver) {
        EchoMessage reply = EchoMessage.newBuilder()
            .setMessage(request.getMessage())
            .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<EchoMessage> echoClientStream(StreamObserver<EchoMessage> responseObserver) {
        return responseObserver;
    }

    @Override
    public void echoServerStream(EchoMessage request, StreamObserver<EchoMessage> responseObserver) {
        EchoMessage reply = EchoMessage.newBuilder()
            .setMessage(request.getMessage())
            .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<EchoMessage> echoStream(StreamObserver<EchoMessage> responseObserver) {
        return responseObserver;
    }
}

