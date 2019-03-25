package demo;

public class ZKLockTest {
    public static Integer i = 0;

    static class TestThread extends Thread {
        private ZKLock lock;

        @Override
        public void run() {
            lock.lock("tl");
            i++;
            lock.unLock("tl");
        }

        public TestThread(ZKLock lock) {
            this.lock = lock;
        }
    }

    public static void testLock() {
        ZKLock lock = ZKLock.getInstance();
        new TestThread(lock).start();
        new TestThread(lock).start();
    }

    public static void main(String[] args) {
        testLock();
    }
}
