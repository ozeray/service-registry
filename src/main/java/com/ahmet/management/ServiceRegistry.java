package com.ahmet.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {

    private static final String SERVICE_REGISTRY_NAMESPACE = "/service_registry";
    private final ZooKeeper zooKeeper;
    private final Logger logger;
    private String currentZnode;
    private List<String> allServiceAddresses;

    public ServiceRegistry(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        logger = LoggerFactory.getLogger(ServiceRegistry.class);
        createServiceRegistryZnode();
    }

    public void registerForUpdates() {
        updateAddresses();
    }

    private void createServiceRegistryZnode() {
        try {
            if (zooKeeper.exists(SERVICE_REGISTRY_NAMESPACE, false) == null) {
                zooKeeper.create(SERVICE_REGISTRY_NAMESPACE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.debug("Race condition");
        }
    }

    public List<String> getAllServiceAddresses() {
        if (allServiceAddresses == null) {
            updateAddresses();
        }
        return allServiceAddresses;
    }

    public void unregisterFromCluster() {
        try {
            if (currentZnode != null && zooKeeper.exists(currentZnode, false) != null) {
                zooKeeper.delete(currentZnode, -1);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Exception occured", e);
        }
    }

    public void registerToCluster(String metadata) throws InterruptedException, KeeperException {
        currentZnode = zooKeeper.create(SERVICE_REGISTRY_NAMESPACE + "/n_", metadata.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.warn("Registered to service registry");
    }

    private synchronized void updateAddresses() {
        try {
            List<String> workerZnodes;
            try {
                workerZnodes = zooKeeper.getChildren(SERVICE_REGISTRY_NAMESPACE, this);
            } catch (KeeperException e) {
                return;
            }

            List<String> addresses = new ArrayList<>(workerZnodes.size());
            for (String workerZnode : workerZnodes) {
                String workerZnodePath = SERVICE_REGISTRY_NAMESPACE + "/" + workerZnode;
                Stat stat;
                try {
                    stat = zooKeeper.exists(workerZnodePath, false);
                } catch (KeeperException e) {
                    continue;
                }
                // If znode dissappeared, do nothing:
                if (stat == null) {
                    continue;
                }

                try {
                    byte[] addressBytes = zooKeeper.getData(workerZnodePath, false, stat);
                    String address = new String(addressBytes);
                    addresses.add(address);
                } catch (KeeperException ignored) { /* znode dissappeared, so do nothing: */ }
            }

            allServiceAddresses = Collections.unmodifiableList(addresses);
            logger.warn("The cluster addresses are: " + allServiceAddresses);
        } catch (InterruptedException e) {
            logger.error("Exception occured" + e);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        updateAddresses();
    }
}
