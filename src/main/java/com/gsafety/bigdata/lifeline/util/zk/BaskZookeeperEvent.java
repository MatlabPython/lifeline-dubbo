package com.gsafety.bigdata.lifeline.util.zk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * @Author: yifeng G
 * @Date: Create in 9:30 2018/3/5 2018
 * @Description:
 * @Modified By:
 * @Vsersion:
 */
public class BaskZookeeperEvent {
    private static final Log LOG = LogFactory.getLog(BaskZookeeperEvent.class);
//    public static final String CLIENT_HOST = "10.5.4.28:2181,10.5.4.29:2181,10.5.4.39:2181";
//    public static final String PATH = "/test/bridge";// 所要监控的结点
    private static ZooKeeper zk;
    private static List<String> nodeList;// 所要监控的结点的子结点列表

    public static void main(String[] args) throws Exception {
        BaskZookeeperEvent baskZookeeperEvent = new BaskZookeeperEvent("10.5.4.28:2181,10.5.4.29:2181,10.5.4.39:2181");
        baskZookeeperEvent.watcherEvent("/test/bridge");

    }

    public BaskZookeeperEvent(String CLIENT_HOST) throws IOException {
        zk = new ZooKeeper(CLIENT_HOST, 21810,new Watcher() {
            public void process(WatchedEvent event) {
            }
        });
    }

    /**
     * 设置watch的线程
     */
    public void watcherEvent(String PATH) {
        Watcher wc = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 结点数据改变之前的结点列表
                List<String> nodeListBefore = nodeList;
                // 主结点的数据发生改变时
                if (event.getType() == Event.EventType.NodeDataChanged) {
                    LOG.info("Node data changed:" + event.getPath());
                    try {
                        byte[] data = zk.getData(event.getPath(), false, null);
                        System.out.println(new String(data));
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(event.getPath().getBytes());
                }
                if (event.getType() == Event.EventType.NodeDeleted) {
                    LOG.info("Node deleted:" + event.getPath());
                }
                if (event.getType() == Event.EventType.NodeCreated) {
                    LOG.info("Node created:" + event.getPath());
                }
                // 获取更新后的nodelist
                try {
                    nodeList = zk.getChildren(event.getPath(), false);
                } catch (KeeperException e) {
                    System.out.println(event.getPath() + " has no child, deleted.");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                List<String> nodeListNow = nodeList;
                // 增加结点
                if (nodeListBefore.size() < nodeListNow.size()) {
                    for (String str : nodeListNow) {
                        if (!nodeListBefore.contains(str)) {
                            LOG.info("Node created:" + event.getPath() + "/" + str);
                        }
                    }
                }
            }
        };

        /**
         * 持续监控PATH下的结点
         */
        while (true) {
            try {
                zk.exists(PATH, wc);//所要监控的主结点
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
            try {
                nodeList = zk.getChildren(PATH, wc);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
            // 对PATH下的每个结点都设置一个watcher
            for (String nodeName : nodeList) {
                try {
                    zk.exists(PATH + "/" + nodeName, wc);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                Thread.sleep(1000);// sleep一会，减少CUP占用率
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * @param threadName
     * @return
     */
    public Thread getThreadByName(String threadName) {
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().equals(threadName)) return thread;
        }
        return null;
    }
}
