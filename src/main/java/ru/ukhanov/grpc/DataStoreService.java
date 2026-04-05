package ru.ukhanov.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

public class DataStoreService extends KeyValueStorageGrpc.KeyValueStorageImplBase {

    private final DataStoreClient backendClient;

    public DataStoreService(DataStoreClient client) {
        this.backendClient = client;
    }

    @Override
    public void insert(InsertRequest req, StreamObserver<InsertResponse> observer) {
        try {
            byte[] payload = req.hasData() ? req.getData().toByteArray() : new byte[0];
            backendClient.insert(req.getId(), payload);
            observer.onNext(InsertResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception ex) {
            observer.onError(Status.INTERNAL
                    .withDescription("Failed to store: " + ex.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void retrieve(RetrieveRequest req, StreamObserver<RetrieveResponse> observer) {
        try {
            var result = backendClient.lookup(req.getId());

            if (!result.found) {
                observer.onError(Status.NOT_FOUND
                        .withDescription("Key '" + req.getId() + "' not exists")
                        .asRuntimeException());
                return;
            }

            var response = RetrieveResponse.newBuilder();
            if (result.bytes != null) {
                response.setData(ByteString.copyFrom(result.bytes));
            }
            observer.onNext(response.build());
            observer.onCompleted();
        } catch (Exception ex) {
            observer.onError(Status.INTERNAL
                    .withDescription("Read failed: " + ex.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void remove(RemoveRequest req, StreamObserver<RemoveResponse> observer) {
        try {
            backendClient.erase(req.getId());
            observer.onNext(RemoveResponse.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception ex) {
            observer.onError(Status.INTERNAL
                    .withDescription("Delete failed: " + ex.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void scan(ScanRequest req, StreamObserver<Record> observer) {
        try {
            backendClient.iterate(req.getStartKey(), req.getEndKey(), (k, v) -> {
                var item = Record.newBuilder().setId(k);
                if (v != null) item.setPayload(ByteString.copyFrom(v));
                observer.onNext(item.build());
            });
            observer.onCompleted();
        } catch (Exception ex) {
            observer.onError(Status.INTERNAL
                    .withDescription("Range scan error: " + ex.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void getTotal(TotalRequest req, StreamObserver<TotalResponse> observer) {
        try {
            long total = backendClient.size();
            observer.onNext(TotalResponse.newBuilder().setTotal(total).build());
            observer.onCompleted();
        } catch (Exception ex) {
            observer.onError(Status.INTERNAL
                    .withDescription("Count error: " + ex.getMessage())
                    .asRuntimeException());
        }
    }
}