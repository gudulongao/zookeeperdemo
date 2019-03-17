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
        Event.KeeperState state = watchedEvent.getState();
        Event.EventType type = watchedEvent.getType();
        System.out.println("listener: " + path + " " + type + " " + state);

        try {
            //再次添加监听（exists操作会对所有的操作都触发）
            zk.exists(path, this);
            zk.getChildren(path, this);
            zk.getData(path, this, null);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("127.0.0.1:2182", 2000, null);

        zk.exists("/test", new MyWatcher(zk));

        zk.setData("/test", "abc".getBytes(), -1);
        zk.setData("/test", "def".getBytes(), -1);
        zk.create("/test/child", "tl".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.delete("/test/child", -1);
    }
}
