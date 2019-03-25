package demo;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class ZKUtilTest extends TestCase {
    public static final String CONNET = "127.0.0.1:2182";
    public static final int SESSION_TIMEOUT = 2000;
    private ZooKeeper zk;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        zk = new ZooKeeper(CONNET, SESSION_TIMEOUT, null);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        zk.close();
    }

    public void testCreateNode() throws KeeperException, InterruptedException {
        ZKUtil.createNode(zk, "/zkroot/zk1/zk11", "abc");
        Assert.assertNotNull(zk.exists("/zkroot/zk1/zk11", false));
    }
}