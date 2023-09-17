package com.ahmet.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class LeaderElection implements Watcher {

    private static final String ELECTION_NAMESPACE = "/election";
    private final ZooKeeper zooKeeper;
    private String currentZnodeName;
    private final Logger logger;

    public LeaderElection(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        logger = LoggerFactory.getLogger(LeaderElection.class);
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.warn("Znode name: " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
        logger.warn("My znode name: " + currentZnodeName);
    }

    public void reelectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;
        String predecessorZnodeName = "";
        while (predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);

            if (smallestChild.equals(currentZnodeName)) {
                logger.warn("I am the leader");
                return;
            } else {
                logger.warn("I'm not the leader, " + smallestChild + " is the leader");

                int currentZnodeIndex = Collections.binarySearch(children, currentZnodeName);

                // I will watch the znode that is just before me in the hierarchy:
                int predecessorIndex = currentZnodeIndex - 1;

                // Find the name of the watched znode:
                predecessorZnodeName = children.get(predecessorIndex);
                // znode indexed with predecessorIndex could be deleted just here. So, we use while (predecessorStat == null).

                // Watch the predecessor znode:
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }

        logger.warn("Watching znode: " + predecessorZnodeName);
    }

    @Override
    public void process(WatchedEvent event) {
        if (Objects.requireNonNull(event.getType()) == Event.EventType.NodeDeleted) {
            try {
                reelectLeader();
            } catch (InterruptedException | KeeperException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
