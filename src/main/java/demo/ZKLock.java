package demo;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * 基于Zookeeper的分布式锁实现
 */
public class ZKLock {
    /**
     * 节点路径 锁的根节点
     */
    private static final String NODEPATH_ROOTLOCK = "/lock";
    /**
     * 节点名称 起始名称
     */
    private static final String NODENAME_BEGIN = "lock_";
    /**
     * 存储于threadlock中的参数 当前序列
     */
    private static final String PARAM_CURRSEQ = "currSeq";
    /**
     * 存储于threadlock中的参数 前序序列
     */
    private static final String PARAM_PRESEQ = "preSeq";
    /**
     * zk锁
     */
    private static ZKLock lock;
    /**
     * zk 连接
     */
    private ZooKeeper zk;
    /**
     * 线程级缓存
     */
    private ThreadLocal<Map<String, String>> threadlocal = new ThreadLocal<Map<String, String>>();


    private ZKLock() {
        zk = ZKHelper.getConn();
    }

    /**
     * 单例
     */
    public static synchronized ZKLock getInstance() {
        if (lock == null) {
            synchronized (ZKLock.class) {
                lock = new ZKLock();
            }
        }
        return lock;
    }

    /**
     * 输出日志
     */
    private static void log(String mess) {
        System.out.println(System.currentTimeMillis() + " " + Thread.currentThread().getId() + " " + mess);
    }

    /**
     * 尝试加锁
     *
     * @param resName 资源名称
     * @return 加锁结果
     */
    public boolean tryLock(String resName) {
        //根据资源名称获取锁的路径
        String lockPath = getLockPath(resName);
        try {
            //判断锁资源对应的节点是否存在
            Stat stat = zk.exists(lockPath, null);
            //节点不存在，则创建永久节点，当前线程成功获取资源锁
            if (stat == null) {
                //创建锁资源节点时，其数据是boolean值，表示当前锁资源是否有线程占有，这个资源锁目前只能被使用一次。
                zk.create(lockPath, String.valueOf(Boolean.TRUE).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log("get resLock!");
                return true;
            }

            //如果当前序列为空，则表明当前线程是第一次尝试加锁。在锁资源节点下创建临时节点。
            //如果当前序列已有，则表明当前线程已经尝试过加锁，但是没能获取到锁，所以再次尝试加锁时不再创建新的临时节点，还是采用上一次已创建好的临时节点来竞争锁
            String currSeq = threadlocal.get().get(PARAM_CURRSEQ);
            if (currSeq == null) {
                String childLockPath = lockPath + "/" + resName;
                currSeq = zk.create(childLockPath, resName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                currSeq = currSeq.substring(currSeq.lastIndexOf("/") + 1);
                //将当前序列加入线程缓存中，用于阻塞后唤起再次尝试加锁
                threadlocal.get().put(PARAM_CURRSEQ, currSeq);
            }
            //获取锁资源节点下的所有节点，由于所资源下都是创建的有序临时节点，故锁资源下的子节点都是按照事件发生的顺序创建的。
            //所有子节点的顺序最小的则是最先请求锁的
            List<String> children = zk.getChildren(lockPath, null);
            log("children: " + children + " currSeq: " + currSeq);
            //排序
            Collections.sort(children);
            //当前序列与最小的子节点进行比对，如果一致，则表明当前线程是最早请求锁的，成功获取锁
            if (currSeq.equals(children.get(0))) {
                log("get currLock: " + currSeq + " !");
                return true;
            }

            //如果当前序列不是最小的，则获取当前序列前一位的序列，即比当前线程早请求的序列
            //如果前序序列不存在，则表明当前线程是第一次尝试加锁，从子节点列表中通过二分查找获取前序序列
            //如果前序序列已存在，则表明当前线程是阻塞唤醒后再次尝试加锁，故判断前序序列是否还存在，如果不存在则重新获取前序序列
            String preSeq = threadlocal.get().get(PARAM_PRESEQ);
            Stat preSeqStat = null;
            if (preSeq != null) {
                preSeqStat = zk.exists(lockPath + '/' + preSeq, false);
            }
            if (preSeqStat == null) {
                preSeq = children.get(Collections.binarySearch(children, currSeq) - 1);
                threadlocal.get().put(PARAM_PRESEQ, preSeq);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 释放锁
     */
    public void unLock(String resName) {
        //获取锁资源路径
        String lockPath = getLockPath(resName);
        try {
            //获取资源锁的值，判断当前线程拥有的是资源锁还是临时锁
            String resLockValue = new String(zk.getData(lockPath, null, null));
            //如果当前线程拥有的是资源锁，则释放资源锁
            if (Boolean.parseBoolean(resLockValue)) {
                Stat stat = zk.exists(lockPath, null);
                zk.setData(lockPath, String.valueOf(Boolean.FALSE).getBytes(), stat.getVersion());
                log("release resLock...");
            }
            //如果当前线程拥有的是临时锁，则删除临时节点，释放临时锁
            else {
                String currSeq = threadlocal.get().get(PARAM_CURRSEQ);
                String childLockPath = lockPath + "/" + currSeq;
                Stat stat = zk.exists(childLockPath, null);
                zk.delete(childLockPath, stat.getVersion());
                log("release currLock:" + currSeq + "...");
            }
            threadlocal.get().put(PARAM_PRESEQ, null);
            threadlocal.get().put(PARAM_CURRSEQ, null);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 等待锁
     */
    private void wait4Lock(String resName) {
        //获取锁资源路径
        String lockPath = getLockPath(resName);
        try {
            //获取资源锁节点的值
            final String resLock = new String(zk.getData(lockPath, null, null));
            final CountDownLatch latch = new CountDownLatch(1);
            //如果当前线程是在等待资源锁的释放，则对资源锁的值变化做监听
            if (String.valueOf(Boolean.TRUE).equals(resLock)) {
                zk.getData(lockPath, new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        if (resLock.equals(watchedEvent.getPath())) {
                            //监听触发后唤醒
                            latch.countDown();
                        }
                    }
                }, null);
                log("wait resLock...");
                //阻塞
                latch.await();
            }
            //如果当前线程是阻塞在临时锁，则对前序节点删除事件加监听
            else {
                String preSeq = threadlocal.get().get(PARAM_PRESEQ);
                final String preNodePath = lockPath + "/" + preSeq;
                zk.exists(preNodePath, new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        if (preNodePath.equals(watchedEvent.getPath())) {
                            //唤醒
                            latch.countDown();
                        }

                    }
                });
                log("wait preSeq" + preSeq + "...");
                //阻塞
                latch.await();
            }
            //再次尝试加锁
            lock(resName, false);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 加锁
     */
    public void lock(String resName) {
        lock(resName, true);
    }

    /**
     * 加锁
     */
    private void lock(String resName, boolean init) {
        if (init) {
            Map<String, String> map = new HashMap<String, String>();
            threadlocal.set(map);
        }
        //尝试加锁，成功则加锁成，否则阻塞等待
        if (tryLock(resName)) {
        } else {
            wait4Lock(resName);
        }
    }

    private static String getLockPath(String resName) {
        return NODEPATH_ROOTLOCK + '/' + NODENAME_BEGIN + resName;
    }

    private static class ZKHelper {
        static final String ZKSERVER = "127.0.0.1:2182";
        static final int SESSIONTIMEOUT = 5000;

        /**
         * 进行zk连接
         */
        public static ZooKeeper getConn() {
            ZooKeeper zk = null;
            try {
                zk = new ZooKeeper(ZKSERVER, SESSIONTIMEOUT, null);
                Stat stat = zk.exists(NODEPATH_ROOTLOCK, null);
                if (stat == null) {
                    zk.create(NODEPATH_ROOTLOCK, "lock".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
            return zk;
        }

        /**
         * 关闭连接
         *
         * @param zk
         */
        public static void close(ZooKeeper zk) {
            if (zk != null && zk.getState().isAlive()) {
                try {
                    zk.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
