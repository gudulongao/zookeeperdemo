package demo;

import org.apache.zookeeper.*;

import java.io.IOException;

public class MyWatcher implements Watcher {
    private ZooKeeper zk;

    public MyWatcher(ZooKeeper zk) {
        this.zk = zk;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        String path = watchedEvent.getPath();
        Event.EventType type = watchedEvent.getType();
        log("watch! path:" + path + " type:" + type);

        try {
            if (Event.EventType.NodeDeleted != type) {
                //再次添加监听（exists操作会对所有的操作都触发）
                zk.exists(path, this);
                zk.getChildren(path, this);
                zk.getData(path, this, null);
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void log(String mess) {
        System.out.println(System.currentTimeMillis() + " " + Thread.currentThread().getName() + " " + mess);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("127.0.0.1:2182", 2000, null);
        log("create node...");
        zk.create("/test", "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        log(" add listener...");
        zk.exists("/test", new MyWatcher(zk));
        log("update data...");
        zk.setData("/test", "abc".getBytes(), -1);
        log("update data...");
        zk.setData("/test", "def".getBytes(), -1);
        log("add child...");
        zk.create("/test/child", "tl".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        log("delete child...");
        zk.delete("/test/child", -1);
        Thread.sleep(1000);
        log("delete node...");
        zk.delete("/te1st", -1);

        Thread.sleep(1000);
//        zk.close();
    }
}
