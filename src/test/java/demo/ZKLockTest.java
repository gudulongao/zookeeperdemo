package demo;

public class ZKLockTest {
    public static Integer i = 0;

    static class TestThread extends Thread {
        private ZKLock lock;

        @Override
        public void run() {
            lock.lock("tl");
            i++;
            log(" result: " + i);
            lock.unLock("tl");

        }

        private void log(String mess) {
            System.out.println(System.currentTimeMillis() + " " + Thread.currentThread().getId() + mess);
        }

        public TestThread(ZKLock lock) {
            this.lock = lock;
        }
    }

    public static void testLock() {
        ZKLock lock = ZKLock.getInstance();
        for (int i = 0; i < 1000; i++) {
            new TestThread(lock).start();
        }
    }

    public static void main(String[] args) {
        testLock();
    }
}
