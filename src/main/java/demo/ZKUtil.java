package demo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class ZKUtil {
    public static void createNode(ZooKeeper zk, String path, String data) throws KeeperException, InterruptedException {
        //判断要创建的节点是否存在
        if (zk.exists(path, false) != null) {
            return;
        }
        //有父级路径，则递归创建父级路径
        String parentPath = path.substring(0, path.lastIndexOf("/"));
        if (parentPath.length() > 0) {
            createNode(zk, parentPath, null);
        }
        //最后创建目标节点
        zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    }

}
