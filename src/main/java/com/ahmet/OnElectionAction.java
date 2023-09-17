package com.ahmet;

import com.ahmet.management.OnElectionCallback;
import com.ahmet.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback {
    private final ServiceRegistry serviceRegistry;
    private final int port;

    public OnElectionAction(ServiceRegistry serviceRegistry, int port) {
        this.serviceRegistry = serviceRegistry;
        this.port = port;
    }

    @Override
    public void onElectedToBeLeader() {
        serviceRegistry.unregisterFromCluster();
        serviceRegistry.registerForUpdates();
    }

    @Override
    public void onElectedAsWorker() {
        try {
            String currentNodeAddress = String.format("http://%s:%d", InetAddress.getLocalHost().getCanonicalHostName(), port);
            serviceRegistry.registerToCluster(currentNodeAddress);
        } catch (UnknownHostException | InterruptedException | KeeperException ignored) { }
    }
}
