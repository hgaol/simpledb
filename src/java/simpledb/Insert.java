package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private DbIterator child;
    private int tableid;
    private TupleDesc td;
    // 影响的tuple数量
    private int count;

    // 是否已经获取了next，道理上讲只能获取一次
    private boolean hasAccessed;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        this.transactionId = t;
        this.child = child;
        this.tableid = tableid;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    /**
     * open是Insert的关键方法，open的时候就把事情做完了
     * todo 最终会调用HeapPage的insertTuple方法将数据写入，但是并没有写入磁盘还(只是内存)，应该有个flush操作吧
     */
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        while (child.hasNext()) {
            Tuple next = child.next();
            try {
                // 插入tuple到指定的tableid中
                Database.getBufferPool().insertTuple(transactionId, tableid, next);
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        hasAccessed = false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (hasAccessed) {
            return null;
        }
        hasAccessed = true;
        Tuple insertedNum = new Tuple(getTupleDesc());
        insertedNum.setField(0, new IntField(count));
        return insertedNum;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child = children[0];
    }
}
