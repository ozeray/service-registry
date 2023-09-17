package com.ahmet.management;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceRegistry {

    private static final String SERVICE_REGISTRY_NAMESPACE = "/service_registry";
    private final ZooKeeper zooKeeper;
    private final Logger logger;

    public ServiceRegistry(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        logger = LoggerFactory.getLogger(ServiceRegistry.class);
        createServiceRegistryZnode();
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

    public void registerToCluster(String metadata) throws InterruptedException, KeeperException {
        zooKeeper.create(SERVICE_REGISTRY_NAMESPACE + "/n_", metadata.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.warn("Node rgistered to service registry");
    }
}
