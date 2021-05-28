package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    private final Map<PageId, PageLock> pageLocks =  new ConcurrentHashMap<>();

    public LockManager() {}

    public void lock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if (!pageLocks.containsKey(pid)) {
            pageLocks.put(pid, new simpledb.storage.PageLock());
        }

        try {
            pageLocks.get(pid).lock(tid, perm);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void unlock(TransactionId tid, PageId pid) {
        if (!pageLocks.containsKey(pid)) {
            return;
        }
        pageLocks.get(pid).unlock(tid);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        if (!pageLocks.containsKey(pid)) {
            return false;
        }
        return pageLocks.get(pid).holdsLock(tid);
    }

}