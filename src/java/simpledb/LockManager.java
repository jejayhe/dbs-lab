package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {
    public enum PageLockType {
        READ,
        WRITE
    }

    public static class PageLock {
        public PageId pid;
        //        public TransactionId tid;
        //        public ReentrantLock mutex; // for upgrading rwLock.readGuard to writeGuard
        public ConcurrentHashMap<TransactionId, PageLockType> txLockMap = new ConcurrentHashMap<>();
        public ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
//        public HashSet<>

        public void RLock(TransactionId tid) {
            PageLockType holdLock = txLockMap.getOrDefault(tid, null);
            if (holdLock == null) {
                rwl.readLock().lock();
                txLockMap.put(tid, PageLockType.READ);
//                Debug.log("setting tid " + tid + " type " + PageLockType.READ);
//                Debug.log("readcount " + rwl.getReadLockCount() + " write count " + rwl.isWriteLocked());
            } else if (holdLock == PageLockType.READ) {
                // pass
            } else {
                // pass
            }
        }

        public void WLock(TransactionId tid) {
            PageLockType holdLock = txLockMap.getOrDefault(tid, null);
            if (holdLock == null) {
                rwl.writeLock().lock();
                txLockMap.put(tid, PageLockType.WRITE);
            } else if (holdLock == PageLockType.READ) {
                Debug.log("code reached 1");
                while (!(rwl.getReadLockCount() == 1 && !rwl.isWriteLocked()))
//                synchronized (this){
                {
                    try {
                        Debug.log("readcount " + rwl.getReadLockCount() + " write count " + rwl.isWriteLocked());
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                    }
                }
                Debug.log("code reached 2");
                rwl = new ReentrantReadWriteLock();
                rwl.writeLock().lock();
                Debug.log("code reached 3");
                txLockMap.put(tid, PageLockType.WRITE);
//                }
            } else {
                // pass
            }
        }

        public void Unlock(TransactionId tid) {
            PageLockType holdLock = txLockMap.getOrDefault(tid, null);
            if (holdLock == null) {
                return;
            } else if (holdLock == PageLockType.READ) {
                rwl.readLock().unlock();
            } else {
                rwl.writeLock().unlock();
            }
            txLockMap.remove(tid);
        }

        public void UnlockRead(TransactionId tid) {
            PageLockType holdLock = txLockMap.getOrDefault(tid, null);
            if (holdLock == PageLockType.READ) {
                rwl.readLock().unlock();
                txLockMap.remove(tid);
            }
        }


//        public PageLockType getHoldLock(TransactionId tid) {
//            return txLockMap.getOrDefault(tid, null);
//        }

        PageLock(PageId pid) {
            this.pid = pid;
        }
    }

    public ConcurrentHashMap<PageId, PageLock> pageLockMap = new ConcurrentHashMap<>();
    public ConcurrentHashMap<TransactionId, HashSet<PageId>> txRelatedPages = new ConcurrentHashMap<>(); // tx modified pages

    public void ReadPage() {

    }

    public void WritePage() {

    }

    public void acquire(TransactionId tid, PageId pid, Permissions perm) {
        Debug.log("acquire tid:" + tid + " pid:" + pid + " perm:" + perm);
        PageLock pageLock = null;
        if (!pageLockMap.containsKey(pid)) {
            pageLock = new PageLock(pid);
            pageLockMap.put(pid, pageLock);
        } else {
            // is there any chance that someone remove the pageLock just before get.
            // in that case, try acquire another time
            pageLock = pageLockMap.get(pid);
        }
        if (perm == Permissions.READ_WRITE) {
            pageLock.WLock(tid);
            if (txRelatedPages.containsKey(tid)) {
                txRelatedPages.get(tid).add(pid);
            } else {
                txRelatedPages.put(tid, new HashSet<PageId>() {{
                    add(pid);
                }});
            }
        } else {
            pageLock.RLock(tid);
        }
    }

    public void release(TransactionId tid, PageId pid) {
        Debug.log("release tid:" + tid + " pid:" + pid);
        PageLock pageLock = pageLockMap.getOrDefault(pid, null);
        if (pageLock == null) {
            return;
        } else {
            pageLock.Unlock(tid);
            txRelatedPages.get(tid).remove(pid);
        }
    }

    public void unlockRead(TransactionId tid, PageId pid) {
        Debug.log("unlockread tid:" + tid + " pid:" + pid);
        PageLock pageLock = pageLockMap.getOrDefault(pid, null);
        if (pageLock != null) {
            pageLock.UnlockRead(tid);
            txRelatedPages.get(tid).remove(pid);
        }
    }

    // for testing purpose
//    public void flush(PageId pid){
//
//    }
}
