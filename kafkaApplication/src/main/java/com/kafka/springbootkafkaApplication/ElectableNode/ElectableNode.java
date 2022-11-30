package com.kafka.springbootkafkaApplication.ElectableNode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
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

class SocketListener implements Runnable {
    private ServerSocket server;
    private Connection conn;

    public SocketListener(ServerSocket server, Connection conn) {
        this.server = server;
        this.conn = conn;
    }

    public void run() {
        while (true) {
            try {
                Socket socket = server.accept();
                Handle h = new Handle(socket, conn);
                Thread thread = new Thread(h);
                thread.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

class Handle implements Runnable {
    private Socket socket;
    private Connection conn;

    public Handle(Socket socket, Connection conn) {
        this.socket = socket;
        this.conn = conn;
    }

    public void run() {
        try {
            handleRequest();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleRequest() throws Exception {
        BufferedReader buffer = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter pWriter = new PrintWriter(socket.getOutputStream(), true);
        String subscribers = "";

        String message = buffer.readLine();

        if (message == null) return;

        System.out.println("Leader listener received query from follower.");
        System.out.println("Query string: " + message);

        pWriter.println("OK");

        ResultSet subscriptionResults = conn.createStatement().executeQuery(message);
        while(subscriptionResults.next()){
            String subscriber = subscriptionResults.getString(1);
            if (subscribers == "") subscribers += subscriber;
            else subscribers += "," + subscriber;
        }

        pWriter.println(subscribers);
    }
}

public class ElectableNode implements Watcher {
    private static final String zookeeperHost = "localhost:2181";
    private static final String znodeRootPath = "/leaderElection";

    private String topic;
    private ZooKeeper zooKeeper;
    private String currZnode;
    private boolean leader;
    private String currLeader;

    private ServerSocket server;
    private Connection conn;

    public ElectableNode(String topic, Connection conn) {
        this.topic = topic;
        this.conn = conn;
    }

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
                    selfVolunteer();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            default:
                break;
        }
    }

    public void selfVolunteer() throws InterruptedException, KeeperException {
        Stat rootStat = zooKeeper.exists(znodeRootPath, false);

        if (rootStat == null) {
			zooKeeper.create(znodeRootPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

        String znodePath = znodeRootPath + "/n_";
        String znodeActualPath = zooKeeper.create(znodePath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("Znode path requested: " + znodePath);
        System.out.println("Znode actual path: " + znodeActualPath);

        currZnode = znodeActualPath.replace(znodeRootPath + "/", "");
    }

    public void elect() throws Exception {
        List<String> children = zooKeeper.getChildren(znodeRootPath, false);

        Collections.sort(children);
        String firstChild = children.get(0);
        Optional<Stat> preZnodeStat = Optional.empty();
        leader = false;

        while (preZnodeStat.isEmpty() && !leader) {
            System.out.println(firstChild);
            System.out.println(currZnode);
            if (firstChild.equals(currZnode)) {
                leader = true;
                currLeader = currZnode;

                System.out.println("Electable node for topic " + topic + " is currently leader.");

                startListening();
            }
            else {
                int currId = Collections.binarySearch(children, currZnode);
                int preZnodeId = currId - 1;
                String preZnode = children.get(preZnodeId);
                leader = false;

                preZnodeStat = Optional.of(zooKeeper.exists(znodeRootPath + "/" + preZnode, this));

                System.out.println("Electable node for topic " + topic + " is currently follower.");
            }
        }
    }

    public void reelect(WatchedEvent e) throws Exception {
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

    public void startListening() throws Exception {
        server = new ServerSocket(1000);
        SocketListener l = new SocketListener(server, conn);
        Thread thread = new Thread(l);
        thread.start();
    }

    public boolean isLeader() {
        return leader;
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
