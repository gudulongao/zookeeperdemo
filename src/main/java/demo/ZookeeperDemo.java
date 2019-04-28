package demo;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class ZookeeperDemo {
    public static final String ADDRESS = "127.0.0.1:2182";

    public static final int SESSION_TIMEOUT = 500;

    private static void log(String mess) {
        System.out.println(System.currentTimeMillis() + " " + Thread.currentThread().getName() + " " + mess);
    }

    /**
     * 获取状态信息
     *
     * @param state
     * @return
     */
    private static String getState(Stat state) {
        StringBuilder builder = new StringBuilder();
        builder.append("Czxid: " + state.getCzxid()).append("\n");
        builder.append("Ctime: " + new Date(state.getCtime())).append("\n");
        builder.append("Mzxid: " + state.getMzxid()).append("\n");
        builder.append("Mtime: " + new Date(state.getMtime())).append("\n");
        builder.append("Pzxid: " + state.getPzxid()).append("\n");
        builder.append("version: " + state.getVersion()).append("\n");
        builder.append("cversion: " + state.getCversion()).append("\n");
        builder.append("aversion: " + state.getAversion()).append("\n");
        builder.append("ephemeralOwner: " + state.getEphemeralOwner()).append("\n");
        builder.append("dataLength: " + state.getDataLength()).append("\n");
        builder.append("numhildren: " + state.getNumChildren()).append("\n");
        return builder.toString();
    }

    /**
     * 基本操作
     */
    public static void testBaseAPI() {
        ZooKeeper client = null;
        try {
            //创建连接
            client = new ZooKeeper(ADDRESS, SESSION_TIMEOUT, null);
            //查询根目录下的节点
            List<String> children = client.getChildren("/", null);
            log("children: " + children);

            client.create("/test", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //获取节点/test的数据
            byte[] data = client.getData("/test", null, null);
            log("data: " + new String(data));
            //获取节点/test的状态数据
            Stat state = client.exists("/test", null);
            log("stat: " + getState(state));

            //更新节点/test的数据，需要基于版本号来更新（乐观锁），如果版本号=-1 则强制更新
            state = client.setData("/test", "tl1".getBytes(), state.getVersion());
            log("stat: " + getState(state));
            data = client.getData("/test", null, null);
            log("stat: " + getState(state));

            state = client.setData("/test", "tl3".getBytes(), -1);
            log("stat: " + getState(state));
            data = client.getData("/test", null, null);
            log("stat: " + getState(state));

            //创建节点
            String result = client.create("/test/child", "child".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode
                    .PERSISTENT);

            client.delete("/test/child", -1);
            client.delete("/test", -1);
            log("result: " + result);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    public static void testWathcer() {
        ZooKeeper client = null;
        try {
            //创建连接（带监听）
            client = new ZooKeeper(ADDRESS, SESSION_TIMEOUT, null);
            client.create("/test", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //通过getData操作添加监听
            //getData的监听只能通过delete和setData触发
            //同时这个监听只能触发一次。如果有两次delete/setData 第二次的操作就不会触发。
            byte[] data = client.getData("/test", new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    log(" path:" + watchedEvent.getPath() + " type: " + watchedEvent.getType());

                }
            }, null);
            log("data: " + new String(data));

            log("delete...");
            client.delete("/test", -1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    public static void testDeleteClose() {
        ZooKeeper client = null;
        try {
            //创建连接
            client = new ZooKeeper(ADDRESS, SESSION_TIMEOUT, null);
            //创建节点
            client.create("/test", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //获取节点状态信息
            Stat stat = client.exists("/test", null);
            //对节点添加监听
            client.getData("/test", new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    log(" path: " + watchedEvent.getPath() + " event: " + watchedEvent.getType());
                }
            }, stat);

            //删除节点
            log("delete...");
            client.delete("/test", -1);

            Thread.sleep(1000);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    public static void main(String[] args) {
//        testBaseAPI();
//        testDeleteClose();
        testWathcer();
    }

}
