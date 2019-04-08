package demo.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;

public class CuratorCacheDemo {
    public static CuratorFramework getWork() {
        return CuratorDemo.getWork();
    }

    public static void testNodeCache() {
        CuratorFramework work = getWork();
        NodeCache nodeCache = new NodeCache(work, "/demo", false);
        NodeCacheListener listener = new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                
            }
        };
    }

    public static void main(String[] args) {
        testNodeCache();
    }
}
