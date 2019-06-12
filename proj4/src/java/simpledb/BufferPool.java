package simpledb;

import java.io.*;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    public static final int PAGE_SIZE = 4096;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int maxPages;

//    private Map<PageId, Page> id2page;
    private LRUCache<PageId, Page> id2page;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        maxPages = numPages;
        // 不使用hashmap了，使用lru cache
        id2page = new LRUCache<>(maxPages);
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        // 命中返回，未命中则加载
        Page page = id2page.get(pid);
        if (page != null) {
            return page;
        } else {
            HeapFile dbFile = (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());
            HeapPage newPage = (HeapPage) dbFile.readPage(pid);
            page = addNewPage(pid, newPage);
            // 如果有需要evict的page，flush它
            if (page != null) {
                try {
                    flushPage(page);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return newPage;
        }
    }

    // NOTE(hgao): 实现了lru cache
    private Page addNewPage(PageId pid, HeapPage newPage) {
        return id2page.put(pid, newPage);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * 最终会调用HeapPage的insertTuple方法，但是这里并没有flush到磁盘上，只是在内存上改变了而已
     * <p>
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock
     * acquisition is not needed for lab2). May block if the lock cannot
     * be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> affectedPages = table.insertTuple(tid, t);
        for (Page page : affectedPages) {
            // 这一步其实已经做过了
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t   the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        int tableId = t.getRecordId().getPageId().getTableId();
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(tableId);
        Page affectedPage = table.deleteTuple(tid, t);
        affectedPage.markDirty(true, tid);
    }

    /**
     * NOTE(hgao): 刷新到磁盘，这里只是为了测试使用，实际的数据库中不应该使用到这个操作
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * 将指定的page刷新到磁盘
     * 因为已经将page驱逐了，从id2page中是找不到这个page的，不如直接flush page
     * Flushes a certain page to disk
     *
     * @param page page to be flushed
     */
    private synchronized void flushPage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        HeapPage dirtyPage = (HeapPage) page;
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(dirtyPage.getId().getTableId());
        table.writePage(dirtyPage);
        dirtyPage.markDirty(false, null);
    }

    /**
     * 根据transactionId刷新到磁盘
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * 该方法在LRUCache中实现了
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    @Deprecated
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
    }

    public int cacheSize() {
        return id2page.size();
    }

}
