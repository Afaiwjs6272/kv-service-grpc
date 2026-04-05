package ru.ukhanov.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StorageServer {
    private static final Logger logger = Logger.getLogger(StorageServer.class.getName());

    private final Server server;
    private final DataStoreClient storeClient;

    public StorageServer(int listenPort, String dbAddress, int dbListenPort,
                         String authUser, String authSecret) throws Exception {

        this.storeClient = new DataStoreClient(dbAddress, dbListenPort, authUser, authSecret);
        this.server = ServerBuilder.forPort(listenPort)
                .addService(new DataStoreService(storeClient))
                .build();
    }

    public void serve() throws Exception {
        server.start();
        logger.info("Storage service listening on port: " + server.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("Shutting down storage service...");
                server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                storeClient.close();
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Shutdown error", e);
            }
        }));

        server.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        int port = 8080;
        String backendHost = "127.0.0.1";
        int backendPort = 3301;
        String credentialUser = "kvuser";
        String credentialPass = "kvpassword";

        new StorageServer(port, backendHost, backendPort, credentialUser, credentialPass).serve();
    }
}