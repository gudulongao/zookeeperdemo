package demo;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class ZookeeperDemo {
    public static final String CONNET = "127.0.0.1:2182";

    public static final int SESSION_TIMEOUT = 500;

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
        try {
            //创建连接
            ZooKeeper client = new ZooKeeper(CONNET, SESSION_TIMEOUT, null);
            //查询根目录下的节点
            List<String> children = client.getChildren("/", null);
            System.out.println(children);

            //获取节点/test的数据
            byte[] data = client.getData("/test", null, null);
            System.out.println(new String(data));

            //获取节点/test的状态数据
            Stat state = client.exists("/test", null);
            System.out.println(getState(state) + "\n");

            //更新节点/test的数据，需要基于版本号来更新（乐观锁），如果版本号=-1 则强制更新
            state = client.setData("/test", "tl1".getBytes(), state.getVersion());
            System.out.println(getState(state));
            data = client.getData("/test", null, null);
            System.out.println(new String(data));

            state = client.setData("/test", "tl3".getBytes(), -1);
            System.out.println(getState(state));
            data = client.getData("/test", null, null);
            System.out.println(new String(data));

            //创建节点
            String result = client.create("/test/child", "child".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public static void testWathcer() {
        try {
            //创建连接（带监听）
            ZooKeeper zk = new ZooKeeper(CONNET, SESSION_TIMEOUT, null);
            zk.create("/test", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //通过getData操作添加监听
            //getData的监听只能通过delete和setData触发
            //同时这个监听只能触发一次。如果有两次delete/setData 第二次的操作就不会触发。
            System.out.println("get data...");
            byte[] data = zk.getData("/test", new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    System.out.println(watchedEvent.getState() + " " + watchedEvent.getPath() + " " + watchedEvent.getType());

                }
            }, null);
            System.out.println("get data: " + new String(data));

//            System.out.println("get data...");
//            data = zk.getData("/test", true, null);
//            System.out.println("get data result: " + new String(data));

//            System.out.println("change data...");
//            zk.setData("/test", "change".getBytes(), -1);

//            System.out.println("chaneg data...");
//            zk.setData("/test", "change2".getBytes(), -1);

            System.out.println("delete node...");
            zk.delete("/test", -1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public static void testDeleteClose() {
        //创建连接（带监听）
        try {
            ZooKeeper zk = new ZooKeeper(CONNET, SESSION_TIMEOUT, null);

            zk.create("/test", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Stat stat = zk.exists("/test", null);
            zk.getData("/test", new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    System.out.println(watchedEvent.getType());
                    System.out.println(watchedEvent.getPath());
                    System.out.println(watchedEvent.getState());
                    System.out.println("delete ...");
                }
            }, stat);
            zk.delete("/test", -1);


//            zk.close();
//
            Thread.sleep(10000);
//            zk.close();
            System.out.println(zk.getState().isAlive());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        testDeleteClose();
//        testWathcer();
    }

}
