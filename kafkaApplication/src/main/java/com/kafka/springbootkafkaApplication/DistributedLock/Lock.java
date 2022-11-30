package com.kafka.springbootkafkaApplication.DistributedLock;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Lock {
    private final static String zookeeperHost = "localhost:2181";
    private final static String rootLockNode = "/lock";
    private static final int timeout = 5000;
    private static final String competitor = "competitorNode";
    private static ZooKeeper zookeeper = null;

    private CountDownLatch latch = new CountDownLatch(1);
    private CountDownLatch getLockLatch = new CountDownLatch(1);

    private String rootPath = null;
    private String lockPath = null;
    private String lockName = null;
    private String competitorPath = null;
    private String actualCompetitorPath = null;
    private String waitedNodePath = null;

    /*
     * Connect with zookeeper host, with lock name.
     * Can have multiple lock with different lock name.
     */
    public void connect(String lockName) throws KeeperException, InterruptedException, IOException {
        Stat rootStat = null;
        Stat lockStat = null;

        if (zookeeper == null) {
			zookeeper = new ZooKeeper(zookeeperHost, timeout,
					new Watcher() {
						@Override
						public void process(WatchedEvent event) {
							if (event.getState().equals(
									KeeperState.SyncConnected)) {
								latch.countDown();
							}

						}
					});
		}

        latch.await();
        rootStat = zookeeper.exists(rootLockNode, false);

        if (rootStat == null) {
			rootPath = zookeeper.create(rootLockNode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} else {
			rootPath = rootLockNode;
		}

        String lockNodePathString = rootLockNode + "/" + lockName;
		lockStat = zookeeper.exists(lockNodePathString, false);

		if (lockStat != null) {
			lockPath = lockNodePathString;
		} else {
			lockPath = zookeeper.create(lockNodePathString, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		this.lockName = lockName;
    }

    private void createNodes() throws KeeperException, InterruptedException {
        competitorPath = lockPath + "/" + competitor;
        actualCompetitorPath = zookeeper.create(competitorPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    private boolean checkConnect() {
        if (StringUtils.isBlank(rootPath) || StringUtils.isBlank(lockName) || zookeeper == null) {
            return false;
        }
        else return true;
    }

    /*
     * Try lock until successful.
     */
    public void tryLock() throws Exception {
        if (!checkConnect()) {
            throw new Exception("Please connect to ZooKeeper first before lock.");
        }

        System.out.println("Start to lock.");

        List<String> competitors = null;
        
        try {
            createNodes();
            competitors = zookeeper.getChildren(lockPath, false);
        } catch (KeeperException e) {
            throw new Exception("Zookeeper connect error.");
        } catch (InterruptedException e) {
            throw new Exception("Try lock interrupted.");
        }

        Collections.sort(competitors);
        int index = competitors.indexOf(actualCompetitorPath.replace(rootLockNode + "/" + lockName + "/", ""));

        if (index == -1) {
            throw new Exception("Competitor path not exist.");
        }
        else if (index == 0) {
            System.out.println("Lock successful.");
            return;
        }
        else {
            waitedNodePath = lockPath + "/" + competitors.get(index - 1);
            Stat waitedNodeStat;
            try {
                waitedNodeStat = zookeeper.exists(waitedNodePath, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getType().equals(EventType.NodeCreated) && event.getPath().equals(waitedNodePath)) {
                            getLockLatch.countDown();
                        }
                    }
                });
                if (waitedNodeStat == null) {
                    System.out.println("Lock successful.");
                    return;
                }
                else {
                    getLockLatch.await();
                    System.out.println("Lock successful.");
                    return;
                }
            } catch (KeeperException e) {
                throw new Exception("Zookeeper connect error.");
            } catch (InterruptedException e) {
                throw new Exception("Try lock interrupted.");
            }
        }
    }

    public void releaseLock() throws Exception {
        if (!checkConnect()) {
            throw new Exception("Please connect to ZooKeeper first before release lock.");
        }

        System.out.println("Start to release lock.");

        try {
            zookeeper.delete(actualCompetitorPath, -1);
        } catch (KeeperException e) {
            throw new Exception("ZooKeeper connect error.");
        } catch (InterruptedException e) {
            throw new Exception("Release lock interrupted.");
        }

        System.out.println("Release lock successful.");
    }

    public static void main(String[] args) {
        Lock dl = new Lock();
        try {
            dl.connect( "testlock");
            dl.tryLock();
            dl.releaseLock();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
