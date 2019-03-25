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
    private static long i = 0;
    /**
     * 节点路径 锁的根节点
     */
    private static final String NODEPATH_ROOTLOCK = "/lock";
    private static final String NODENAME_BEGIN = "lock_";
    private static ZKLock lock;
    private ZooKeeper zk;
    private ThreadLocal<Map<String, String>> threadlocal = new ThreadLocal<Map<String, String>>();


    private ZKLock() {
        zk = ZKHelper.getConn();
    }

    public static synchronized ZKLock getInstance() {
        if (lock == null) {
            synchronized (ZKLock.class) {
                lock = new ZKLock();
            }
        }
        return lock;
    }

    public boolean tryLock(String resName) {
        String lockPath = getLockPath(resName);
        try {
            Stat stat = zk.exists(lockPath, null);
            if (stat == null) {
                zk.create(lockPath, String.valueOf(Boolean.TRUE).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                threadlocal.get().put("isFirst", String.valueOf(Boolean.TRUE));
                System.out.println(System.currentTimeMillis() + " " + Thread.currentThread().getId() + " get resLock!");
                return true;
            }

            String childLockPath = lockPath + "/" + resName;
            String currSeq = zk.create(childLockPath, resName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            currSeq = currSeq.substring(currSeq.lastIndexOf("/") + 1);
            threadlocal.get().put("currSeq", currSeq);
            System.out.println("currSeq:" + currSeq);

            List<String> children = zk.getChildren(lockPath, null);
            System.out.println("child: " + children + " currSeq: " + currSeq);
            Collections.sort(children);
            if (currSeq.equals(children.get(0))) {
                System.out.println(System.currentTimeMillis() + " " + Thread.currentThread().getId() + " get currLock[" + currSeq + "]");
                return true;
            }

            String preSeq = children.get(Collections.binarySearch(children, currSeq) - 1);
            threadlocal.get().put("preSeq", preSeq);
            System.out.println("preSeq:" + preSeq);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void unLock(String resName) {
        String lockPath = getLockPath(resName);
        try {
            if (Boolean.parseBoolean(threadlocal.get().get("isFirst"))) {
                threadlocal.get().put("isFirst", String.valueOf(Boolean.FALSE));
                Stat stat = zk.exists(lockPath, null);
                zk.setData(lockPath, String.valueOf(Boolean.FALSE).getBytes(), stat.getVersion());
                System.out.println(System.currentTimeMillis() + " " + Thread.currentThread().getId() + " release resLock");
            } else {
                String currSeq = threadlocal.get().get("currSeq");
                String childLockPath = lockPath + "/" + currSeq;
                Stat stat = zk.exists(childLockPath, null);
                zk.delete(childLockPath, stat.getVersion());
                System.out.println(System.currentTimeMillis() + " " + Thread.currentThread().getId() + " release currLock[" + currSeq + "]");
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void wait4Lock(String resName) {
        String lockPath = getLockPath(resName);
        try {
            final String resLock = new String(zk.getData(lockPath, null, null));
            final CountDownLatch latch = new CountDownLatch(1);
            System.out.println(System.currentTimeMillis() + " " + Thread.currentThread().getId() + " wait resLock...");
            if (String.valueOf(Boolean.TRUE).equals(resLock)) {
                zk.getData(lockPath, new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        if (resLock.equals(watchedEvent.getPath())) {
                            latch.countDown();
                        }
                    }
                }, null);
                latch.await();
            } else {
                String preSeq = threadlocal.get().get("preSeq");
                final String preNodePath = lockPath + "/" + preSeq;
                System.out.println(System.currentTimeMillis() + " " + Thread.currentThread().getId() + " wait preSeq[" + preNodePath + "] release lock...");
                zk.exists(preNodePath, new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        if (preNodePath.equals(watchedEvent.getPath())) {
                            latch.countDown();
                        }

                    }
                });
                latch.await();
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void lock(String resName) {
        Map<String, String> map = new HashMap<String, String>();
        threadlocal.set(map);
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
