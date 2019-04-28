package demo.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

public class CuratorCacheDemo {
    /**
     * zk 服务地址
     */
    private static final String ZK_SERVER = "127.0.0.1:2182";

    /**
     * 获取连接
     *
     * @return
     */
    public static CuratorFramework getWork() {
        //基于/demo为基准路径创建会话
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().connectString
                (ZK_SERVER)
                .sessionTimeoutMs(60000)
                .connectionTimeoutMs(20000).retryPolicy(new ExponentialBackoffRetry(3000, 2)).build();
        curatorFramework.start();
        return curatorFramework;
    }

    /**
     * 获取缓存监听
     *
     * @param nodeCache
     * @return
     */
    public static NodeCacheListener getNodeCacheListener(final NodeCache nodeCache) {
        return new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                ChildData currentData = nodeCache.getCurrentData();
                System.out.println("path:" + currentData.getPath());
                System.out.println("data:" + new String(currentData.getData()));
                System.out.println("-----------");
            }
        };
    }

    public static void testNodeCache() throws Exception {
        //获取连接
        CuratorFramework work = getWork();
        String nodePath = "/demo/demochild";
        //构建NodeCache
        final NodeCache nodeCache = new NodeCache(work, nodePath, false);
        //开始缓存
        //true nodecache会检查节点是否存在，同时立即缓存节点数据
        //之后与zk进行比对的时候，就不会因为缓存中没有数据而zk有的不同变化而触发监听。可以看start的源码
        nodeCache.start(true);
        //添加监听
        nodeCache.getListenable().addListener(getNodeCacheListener(nodeCache));

        //测试几次注册，永久监听的效果
        Thread.sleep(1000);
        work.setData().forPath(nodePath, "abc".getBytes());
        Thread.sleep(1000);
        work.setData().forPath(nodePath, "edf".getBytes());
        Thread.sleep(1000);
        work.setData().forPath(nodePath, "ccc".getBytes());
        Thread.sleep(1000);
        //测试删除会不会触发监听
        work.delete().deletingChildrenIfNeeded().forPath(nodePath);
        Thread.sleep(3000);
        nodeCache.close();
        work.close();
    }

    private static PathChildrenCacheListener getPathChildrenCacheListener() {
        return new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent)
                    throws Exception {
                System.out.println("type: " + pathChildrenCacheEvent.getType());
                System.out.println("path: " + pathChildrenCacheEvent.getData().getPath());
                System.out.println("data: " + new String(pathChildrenCacheEvent.getData().getData()));
                System.out.println("----------");
            }
        };
    }

    public static void testPathChildrenCache() throws Exception {
        CuratorFramework work = getWork();
        String path = "/demo/demochild";
        //构建子节点缓存
        PathChildrenCache cache = new PathChildrenCache(work, path, true);
        //开启缓存
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        //添加缓存
        cache.getListenable().addListener(getPathChildrenCacheListener());

        Thread.sleep(1000);
        //创建子节点
        work.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path + "/a", "axi".getBytes());
        Thread.sleep(1000);
        //创建孙子节点
        work.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path + "/a/b", "bxi".getBytes());
        Thread.sleep(1000);
        //修改孙子节点
        work.setData().forPath(path + "/a/b", "b".getBytes());
        Thread.sleep(1000);
        //修改子节点
        work.setData().forPath(path + "/a", "a".getBytes());
        Thread.sleep(1000);
        //删除孙子节点
        work.delete().forPath(path + "/a/b");
        Thread.sleep(1000);
        //删除子节点
        work.delete().forPath(path + "/a");
        Thread.sleep(1000);
        //关闭缓存
        cache.close();
        System.out.println("-----close cache--------");
        Thread.sleep(1000);
        //创建子节点
        work.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path + "/a");
        System.out.println("-----create node--------");
        Thread.sleep(1000);
        //删除子节点
        work.delete().forPath(path + "/a");
        System.out.println("-----delete node--------");
        Thread.sleep(1000);
        work.close();
    }

    public static void main(String[] args) throws Exception {
//        testNodeCache();
        testPathChildrenCache();
    }
}
