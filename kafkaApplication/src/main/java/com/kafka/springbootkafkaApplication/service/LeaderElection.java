package com.kafka.springbootkafkaApplication.service;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class LeaderElection implements Watcher {
    private static final String zookeeperHost = "localhost:8088";
    private static final String znodeRootPath = "/leaderElection";

    private ZooKeeper zooKeeper;
    private String currZnode;
    private boolean leader;

    private String currLeader;

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Connected to Zookeeper");
                }
                else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper.");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                try {
                    reelect(event);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case NodeCreated:
                try {
                    createZnode();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            default:
                break;
        }
    }

    public void createZnode() throws InterruptedException, KeeperException {
        String znodePath = znodeRootPath + "/n_";
        String znodeActualPath = zooKeeper.create(znodePath, new byte[]{}, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("Znode path requested: " + znodePath);
        System.out.println("Znode actual path: " + znodeActualPath);

        currZnode = znodeActualPath.replace(znodeRootPath + "/", "");
    }

    public void elect() throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren(znodeRootPath, false);

        Collections.sort(children);
        String firstChild = children.get(0);
        Optional<Stat> preZnodeStat = Optional.empty();
        leader = false;

        while (preZnodeStat.isEmpty() && !leader) {
            if (firstChild == currZnode) {
                leader = true;
                currLeader = currZnode;
            }
            else {
                int currId = Collections.binarySearch(children, currZnode);
                int preZnodeId = currId - 1;
                String preZnode = children.get(preZnodeId);

                preZnodeStat = Optional.of(zooKeeper.exists(znodeRootPath + "/" + preZnode, this));
            }
        }
    }

    public void reelect(WatchedEvent e) throws InterruptedException, KeeperException {
        String changedNode = e.getPath().replace(znodeRootPath + "/", "");

        // if removed node or crashed node is not current leader
        if (!currLeader.equalsIgnoreCase(changedNode)) {
            return;
        }

        // if removed node or crashed node is current leader
        if (zooKeeper.getChildren(znodeRootPath, false).size() == 0) {
            System.out.println("Cluster has 0 node.");
            return;
        }

        elect();
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void connect() throws IOException {
        this.zooKeeper = new ZooKeeper(zookeeperHost, 3000, this);
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}
