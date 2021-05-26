package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

public class PageLock {

    enum Mode {
        UNLOCKED,
        SHARED,
        EXCLUSIVE
    }

    private final Set<TransactionId> owners = new HashSet<>();
    private Mode mode;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition cond = lock.newCondition();

    public PageLock() {
        mode = Mode.UNLOCKED;
    }

    public void lock(TransactionId tid, Permissions perm) throws InterruptedException {
        if (perm == Permissions.READ_ONLY) {
            sLock(tid);
        } else {
            xLock(tid);
        }
    }

    public void unlock(TransactionId tid) {
        lock.lock();
        try {
            owners.remove(tid);
            if (owners.size() == 0) {
                mode = Mode.UNLOCKED;
            }
            cond.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public boolean holdsLock(TransactionId tid) {
        lock.lock();
        try {
            return owners.contains(tid);
        } finally {
            lock.unlock();
        }
    }

    private void sLock(TransactionId tid) throws InterruptedException {
        lock.lock();

        try {
            while (true) {
                if (owners.contains(tid)) {
                    break;
                }
                if (mode != Mode.EXCLUSIVE) {
                    owners.add(tid);
                    mode = Mode.SHARED;
                    break;
                }
                cond.await();
            }
        } finally {
            if (lock.isLocked()) {
                lock.unlock();
            }
        }
    }

    private void xLock(TransactionId tid) throws InterruptedException {
        lock.lock();

        try {
            while (true) {
                if (owners.contains(tid)) {
                    if (mode == Mode.EXCLUSIVE) {
                        break;
                    }
                    if (mode == Mode.SHARED) {
                        if (owners.size() == 1) {
                            mode = Mode.EXCLUSIVE;
                            break;
                        }
                        owners.remove(tid);
                    }
                }
                if (mode == Mode.UNLOCKED) {
                    mode = Mode.EXCLUSIVE;
                    owners.add(tid);
                    break;
                }
                cond.await();
            }
        } finally {
            if (lock.isLocked()) {
                lock.unlock();
            }
        }
    }
}