package demo;

public class ZKLockTest {
    public static Integer i = 0;

    static class TestThread extends Thread {
        private ZKLock lock;

        @Override
        public void run() {
            lock.lock("tl");
            i++;
            System.out.println(System.currentTimeMillis() + " " + Thread.currentThread().getId() + " result: " + i);
            lock.unLock("tl");

        }

        public TestThread(ZKLock lock) {
            this.lock = lock;
        }
    }

    public static void testLock() throws InterruptedException {
        ZKLock lock = ZKLock.getInstance();
        for (int i = 0; i < 1000; i++) {
            new TestThread(lock).start();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        testLock();
    }
}
