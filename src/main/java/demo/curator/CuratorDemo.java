package demo.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CuratorDemo {
    /**
     * zk 服务地址
     */
    private static final String ZK_SERVER = "127.0.0.1:2182";

    public static void testCreateSession() {
        //定义重试策略
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3, Integer.MAX_VALUE);

        //指定会话超时时长，连接超时时长以及重试策略
        CuratorFrameworkFactory.newClient(ZK_SERVER, 8000, 12000, retryPolicy).start();

        //采用默认的会话超时6000 和连接超时15000
        CuratorFrameworkFactory.newClient(ZK_SERVER, retryPolicy).start();

        //Fluent风格，namespace是指定会话中所有的操作的基准目录路径，后续如果对/demo操作实际是对/lock/demo操作
        CuratorFrameworkFactory.builder().connectString(ZK_SERVER).sessionTimeoutMs(8000).connectionTimeoutMs(12000)
                .retryPolicy(retryPolicy).namespace("/lock").build().start();
    }


    private static CuratorFramework getWork() {
        //定义重试策略
        RetryOneTime retryOneTime = new RetryOneTime(30000);

        //基于/demo为基准路径创建会话
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().connectString
                (ZK_SERVER)
                .sessionTimeoutMs(60000)
                .connectionTimeoutMs(20000).namespace("demo").retryPolicy(retryOneTime).build();
        curatorFramework.start();
        return curatorFramework;
    }

    public static void testCreateNode() throws Exception {
        CuratorFramework work = getWork();
        //创建持久的节点/demo/demochild 支持递归创建
        work.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath("/demochild", "child".getBytes());
    }

    public static void testBackOper() throws Exception {
        CuratorFramework work = getWork();
        System.out.println("main thread:" + Thread.currentThread().getName());
        //异步的方式创建节点
        work.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws
                            Exception {
                        System.out.println("code:" + curatorEvent.getResultCode() + " type:" + curatorEvent.getType());
                        System.out.println("context:" + curatorEvent.getContext());
                        System.out.println("thread id:" + Thread.currentThread().getId() + " thread name:" + Thread
                                .currentThread().getName());
                    }
                }).forPath("/demobackground");
        Thread.sleep(3000);

        //异步删除节点
        @SuppressWarnings("AlibabaThreadPoolCreation")
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        work.delete().deletingChildrenIfNeeded().withVersion(-1).inBackground(new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                System.out.println("code:" + curatorEvent.getResultCode() + " type:" + curatorEvent.getType());
                System.out.println("context:" + curatorEvent.getContext());
                System.out.println("thread id:" + Thread.currentThread().getId() + " thread name:" + Thread
                        .currentThread().getName());
            }
        }, "test context", executorService).forPath("/demobackgroud");
        Thread.sleep(3000);

        executorService.shutdown();
    }

    public static void testTransaction() throws Exception {
        CuratorFramework work = getWork();
    }

    public static void testDeleteNode() throws Exception {
        CuratorFramework work = getWork();
        //递归删除
        work.delete().deletingChildrenIfNeeded().forPath("/demochild");
    }

    public static void main(String[] args) throws Exception {
//        testCreateNode();
//        testDeleteNode();
        testBackOper();
    }
}
