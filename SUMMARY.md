# SUMMARY

## Project 1
project1 实现了很多的Class来表示元组等，这里我打算直接从一个类似SQL的入口类来说名proj1做了什么事情，这个入口类包含了大多数的proj1的实现。
```sql
SQL statement SELECT * FROM some_data_file
```
等价的java类
```java
public class test {
    public static void main(String[] argv) {

        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);
        
        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");
        
        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());
        
        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }
}
```

接下来会跟着这个类，一点一点的解释。

### 创建table的schema信息
TupleDesc主要保存table的schema信息，如下所示，创建了一个3列都是int类型，field0...2为每个列的名称。
```java
// construct a 3-column table schema
Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
String names[] = new String[]{ "field0", "field1", "field2" };
TupleDesc descriptor = new TupleDesc(types, names);
```
### 从磁盘读取数据信息
从磁盘创建HeapFile，然后添加到Database中
```java
// create the table, associate it with some_data_file.dat
// and tell the catalog about the schema of this table.
HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
Database.getCatalog().addTable(table1, "test");
```

看看 `HeapFile` 是什么，下面列出主要方法和成员变量
```java
// HeapFile的iterator，见下
HeapFileIterator
// 构造方法，传入file和tuple desc
HeapFile(File f, TupleDesc td);
// 读取页(真正和磁盘打交道的地方)，一页默认4096Byte，其中pid保存第几页信息，直接seek过去读取
Page readPage(PageId pid);
// 写入页(真正和磁盘打交道的地方)，同上，page包含有第几页，直接seek过去写入
void writePage(Page page);
// 根据文件的大小，看看有多少页（ceil(size / page_size)）
int numPages();
// 插入一个tuple。如果前面有空槽，写空槽里；否则新建一个页。会从BufferPool获取页(所有读取页的操作都会从BufferPool走)
// 写完会在tuple中写入record信息(主要保存文件id和页数信息)。
// 返回受到影响的页
ArrayList<Page> insertTuple(TransactionId tid, Tuple t);
// 删除一个tuple。tuple中含有文件和页数的信息。会从BufferPool获取页。
// 返回受影响的页
Page deleteTuple(TransactionId tid, Tuple t);
// 相关成员变量
private TupleDesc td;
private File f;
private int numPage
```
后续会有transaction相关的操作，所以insertTuple和deleteTuple都会从BufferPool中走，便于统一事务管理(此处先不表)。

`HeapFileIterator` 
```java
private int pagePos;
private Iterator<Tuple> tuplesInPage;
private TransactionId tid;
//
HeapFileIterator(TransactionId tid);
// pagePos = 0, 加载页并赋值给tuplesInPage
void open();
// 从buffer pool中读取HeapPage，返回page.iterator(), HeapPage的iterator见下，其功能就是返回这个page的所有tuples
Iterator<Tuple> getTuplesInPage(HeapPageId pid);
// 首先检查当前page.hasNext()，没有则检查是不是最后一页，是则返回false，否则赋值下一页的iterator
boolean hasNext();
// tuplesInPage.next();
Tuple next();
```

再来看看 `HeapPage` ，因为 `HeapFile` 的许多iterator相关的操作，实际都是使用的heap page的
```java
// 该heap page所在的页(对应物理上磁盘位置)
HeapPageId pid;
// 该页的tuple desc，所以一个页都应该是一种schema
TupleDesc td;
// header
byte header[];
// data
Tuple tuples[];
// 有多少slots，即tuples和header的大小
int numSlots;
// 磁盘上的旧数据
byte[] oldData;
// 当前page被哪个transactionid持有
private TransactionId lastDirtyOperation;
// 构造函数，初始化pid/td/numSlots，从data中构建header和tuples信息，并初始化。oldData = data
HeapPage(HeapPageId id, byte[] data);
// ((BufferPool.PAGE_SIZE * 8) /  (td.getSize() * 8 + 1))
int getNumTuples();
// Math.ceil(this.numSlots / 8.0)
int getHeaderSize();
// 读取dis信息构建tuple
private Tuple readNextTuple(DataInputStream dis, int slotId);
// 删除某个tuple
public void deleteTuple(Tuple t);
// 找到一个空闲的slot，插入tuple，并设置相应的header
public void insertTuple(Tuple t);
// 返回当前tuples的一个iterator
Iterator<Tuple> iterator();
```

接下来看看 `Catalog` 这个类的主要函数(Catalog主要保存目前数据库中的表信息)
```java
// table和id的对应关系，这里的id是file的id，也就是file绝对路径的hashcode
private Map<Integer, Table> id2table;
private Map<String, Integer> name2id;
// 增加一个table，更新上面2个变量，这里用到了Table这个类，见下面描述
void addTable(DbFile file, String name, String pkeyField);
// id2table.clear(); & name2id.clear();
void clear();
// 这个方法比较关键，最初读取schema文件的时候，都是这个方法
// 简单来说，这个方法读取schema文件，文件内容类似这样 table_name (field1 type1, field2 type2, ...)
// --------
// authors (id int, name string)
// venues (id int, name string, year int, type int)
// papers (id int, title string, venueid int)
// paperauths (paperid int, authorid int)
// --------
// 1. 逐行读取table schema，找到schema父目录下同名.dat文件作为data文件，例如 authors.dat
// 2. 接下来两行和之前如出一辙，读取data文件，加入到Catalog中
// 	HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
// 	addTable(tabHf, name, primaryKey);
void loadSchema(String catalogFile);
```

`Table` 
```java
private DbFile dbFile; // 文件
private String name; // table名称
private String pkeyField; // 主键名称
```

### 扫描data文件中的tuple
从磁盘读取了schema和data信息后，就可以使用seqscan迭代的读取数据了，也就是select的操作
```java
// construct the query: we use a simple SeqScan, which spoonfeeds
// tuples via its iterator.
TransactionId tid = new TransactionId();
SeqScan f = new SeqScan(tid, table1.getId());

try {
    // and run it
    f.open();
    while (f.hasNext()) {
        Tuple tup = f.next();
        System.out.println(tup);
    }
    f.close();
    Database.getBufferPool().transactionComplete(tid);
} catch (Exception e) {
    System.out.println ("Exception : " + e);
}
```
`TransactionId` 此处先不管，之后事务会讲。一起看下 `SeqScan` 类吧

`SeqScan` 主要函数和变量，大部分操作都是直接使用heapfile的iterator
```java
// 变量
private TransactionId tid;
private int tableid;
private String tableAlias;
private DbFileIterator iterator;
// catalog.dbfile(tableid).iterator，返回heapfile的iterator
SeqScan(TransactionId tid, int tableid, String tableAlias);
public SeqScan(TransactionId tid, int tableid) {
    this(tid, tableid, Database.getCatalog().getTableName(tableid));
}
// iterator.open();
void open();
```
至此，基本覆盖到了project 1所用到的主要的类。执行结果如图：<br />![image.png](https://cdn.nlark.com/yuque/0/2019/png/135791/1560777236876-3a081437-73b8-4868-94c1-bf276da16896.png#align=left&display=inline&height=93&name=image.png&originHeight=232&originWidth=1120&size=28905&status=done&width=448)

## Project 2
这个project主要完成各种operator，包括insert，delete，selections，join和aggregates(group by)。这些方法将在project 1实现的框架的上层完成。本次还将完成一个LRUcache来保存buffer pool中的页。

接下来还是从一个入口函数开始
```java
public class JoinMainTest {

    public static void main(String[] argv) {
        // construct a 3-column table schema
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1", "field2"};
        if (argv.length != 2) {
            System.out.println("Illegal arguments");
            System.out.println("  ./program file1.dat file2.dat");
            System.exit(1);
        }

        System.out.println(Arrays.toString(argv));
        TupleDesc td = new TupleDesc(types, names);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File(argv[0]), td);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File(argv[1]), td);
        Database.getCatalog().addTable(table2, "t2");

        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition
        Filter sf1 = new Filter(
                new Predicate(0,
                        Predicate.Op.GREATER_THAN, new IntField(1)), ss1);

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);

        // and run it
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
```

### Filter和Predicate
前面大多数proj1都提到过，下面看看这一行都干了什么
```java
// create a filter for the where condition
Filter sf1 = new Filter(
        new Predicate(0,
                Predicate.Op.GREATER_THAN, new IntField(1)), ss1);
```
这一行等价于 `WHERE` 之后的语句，比如 `WHERE some_data_file1.id > 1` ，接下来进去看看<br />Filter继承自Operator，包装Predicate来实现更多的功能。<br />`Filter` 
```java
// 传入的具体的操作
private Predicate p;
// 一般是seqscan，也可能是【责任链模式】的前一个结果
private DbIterator child;
// 把过滤的结果缓存下来
private TupleIterator filterResult;
// 构造函数
public Filter(Predicate p, DbIterator child);
// 调用filter，将filter的结果传给filterResult，并open()
public void open();
// 对每一个tuple，执行p.filter(tuple)，true则添加到结果中
private TupleIterator filter(Predicate p, DbIterator child);
// 其他都是一些简单的方法
```

`Predicate` 
```java
// 比较符号，有 EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS
private Op op;
// 待比较的field的index
private int field;
// 操作数，
private Field operand;
// 具体的比较操作，在IntField和StringField中有定义
public boolean filter(Tuple t) { return t.getField(field).compare(op, operand); }
```

### Join
Filter继承自Operator，对两个表满足的关系进行合并。
```java
// join的predicate，等价于 where t1.id = t2.id，p.filter(tup1, tup2)
private JoinPredicate p;
// 待合并的两个tuples
private DbIterator child1;
private DbIterator child2;
// 合并之后的tupledesc，就是将两个表拼接一起
private TupleDesc td;
// 保存合并后的结果
private TupleIterator filterResult;
// 主要介绍filter。遍历两个iter，判断是否满足p.filter(tup1, tup2)，满足则合并并加入到filterResult中
private TupleIterator filter(JoinPredicate p, DbIterator child1, DbIterator child2);
```
JoinPredicate比较简单，就不赘述。

### Insertion and deletion
这两个都很简单，直接看 `Insert` 
```java
// 
private TransactionId transactionId;
private DbIterator child;
private int tableid;
private TupleDesc td;
// 影响的tuple数量
private int count;
// 是否完成插入标志
private boolean hasAccessed;
// 会调用buffer pool中的insertTuple的方法(记得这个方法最终都会调用getPage，进而保证事务)
public void open();
```
`Delete` 和 `Insert` 类似，就不赘述。

最后是一张执行select的截图<br />![image.png](https://cdn.nlark.com/yuque/0/2019/png/135791/1560838265339-790c7ac1-6788-4df4-ada4-8a0e8b11e0a4.png#align=left&display=inline&height=535&name=image.png&originHeight=1338&originWidth=1312&size=128431&status=done&width=524.8)

## Project 3
proj3主要实现查询优化的工作。包括查询预估框架和Selinger cost-based优化器。盗一张图<br />![image.png](https://cdn.nlark.com/yuque/0/2019/png/135791/1560838976486-681c7054-b9b8-4e33-b3cd-f1f6f95c0b93.png#align=left&display=inline&height=663&name=image.png&originHeight=400&originWidth=300&size=46438&status=done&width=497)<br />我们从这个例子开始看
```shell
$ java -jar simpledb.jar parser 0.01/imdb.schema -explain
Added table : Actor with schema < id(INT_TYPE) fname(STRING_TYPE) lname(STRING_TYPE) gender(STRING_TYPE) >
Added table : Movie with schema < id(INT_TYPE) name(STRING_TYPE) year(INT_TYPE) >
Added table : Director with schema < id(INT_TYPE) fname(STRING_TYPE) lname(STRING_TYPE) >
Added table : Casts with schema < pid(INT_TYPE) mid(INT_TYPE) role(STRING_TYPE) >
Added table : Movie_Director with schema < did(INT_TYPE) mid(INT_TYPE) >
Added table : Genre with schema < mid(INT_TYPE) genre(STRING_TYPE) >
Computing table stats.
Done.
Explain mode enabled.
SimpleDB> select d.fname, d.lname
SimpleDB> from Actor a, Casts c, Movie_Director m, Director d
SimpleDB> where a.id=c.pid and c.mid=m.mid and m.did=d.id 
SimpleDB> and a.fname='John' and a.lname='Spicer';
```
结果<br />![image.png](https://cdn.nlark.com/yuque/0/2019/png/135791/1560840191242-80287b61-bccd-44df-be07-170d9a695fe9.png#align=left&display=inline&height=749&name=image.png&originHeight=1872&originWidth=1510&size=201155&status=done&width=604)<br />比如我们有个plan要执行如下操作 p=t1 join t2 join ... tn ，那么这个plan消耗的时间预估为：<br />iocost(t1) + iocost(t2) + cpucost(t1 join t2) +<br />iocost(t3) + cpucost((t1 join t2) join t3) +<br />...<br />其中iocost是从磁盘读取这个tuples的损耗，一般可以认为 tuples_size / page_size。cpucost(t1 join t2)是一个O(nm)的时间复杂度的cpu计算。<br />当我们使用**nested loops joins**，损耗可以这么计算：<br />joincost(t1 join t2) = scancost(t1) + _ntups(t1) x scancost(t2)_ //IO cost<br />                       + ntups(t1) x ntups(t2)  //CPU cost<br />我还还需要知道某个映射操作之后还剩多少个tuple，这在join optimizer会用到。<br />可以参考[这里](http://www.mathcs.emory.edu/~cheung/Courses/554/Syllabus/5-query-opt/left-deep-trees.html)。

我们使用Selinger-style optimizer进行join优化。主要通过动态规划算法，获得某个排列<R ⋈ R ⋈ R ... ⋈ R>的最优join顺序。这里我们采用left deep join的方式，关于其概念，可参考上面。对于具体算法，直接引用课程页面

> 1. j = set of join nodes
> 2. for (i in 1...|j|):  // First find best plan for single join, then for two joins, etc. 
> 3.     for s in {all length i subsets of j} // Looking at a concrete subset of joins
> 4.       bestPlan = {}  // We want to find the best plan for this concrete subset 
> 5.       for s' in {all length i-1 subsets of s} 
> 6.            subplan = optjoin(s')  // Look-up in the cache the best query plan for s but with one relation missing

> 7.            plan = best way to join (s-s') to subplan // Now find the best plan to extend s' by one join to get s

> 8.            if (cost(plan) < cost(bestPlan))

> 9.               bestPlan = plan // Update the best plan for computing s

> 10.      optjoin(s) = bestPlan

> 11. return optjoin(j)


与之对应的函数是
```java
public Vector<LogicalJoinNode> orderJoins(
        HashMap<String, TableStats> stats,
        HashMap<String, Double> filterSelectivities, boolean explain)
```

## Project 4
第四章主要实现事务。包括乐观锁(读锁)和悲观锁(写锁)的实现，严格两阶段封锁协议，**NO STEAL/FORCE **buffer管理协议（1. 不能驱逐某个被事务占据的页；2. 事务结束后，相关的页必须刷新到磁盘），还实现了死锁检测(图检测的变种，相对于图检测更简单)。<br />主要的入口在 `simpledb.BufferPool#getPage` 这里，这里把相关代码附上
```java
// 首先检测该pid是否可以被当前tid获取锁（写锁or读锁）
boolean result = (perm == Permissions.READ_ONLY) ? lockManager.grantSLock(tid, pid) : lockManager.grantXLock(tid, pid);
// 下面的while循环就是在模拟等待过程，如果没有获取到锁，隔一段时间就检查一次是否申请到锁了，还没申请到就检查是否陷入死锁
while (!result) {
    // 死锁检测
    if (lockManager.deadlockOccurred(tid, pid)) {
        throw new TransactionAbortedException();
    }
    try {
        Thread.sleep(SLEEP_INTERVAL);
    } catch (InterruptedException e) {
        e.printStackTrace();
    }
    //sleep之后再次判断result
    result = (perm == Permissions.READ_ONLY) ? lockManager.grantSLock(tid, pid) : lockManager.grantXLock(tid, pid);
}
```
后面还有一部分是实现**NO STEAL**协议，当lrucache中已经满了，会看将要驱逐的页是否dirty，如果dirty，驱逐其他页，如果全部dirty，报错。<br />还有很多细节就不意义展示了，有兴趣的可以阅读相关代码。

## 总结
simpledb是一个很简易的数据库，虽然还有很多功能没有实现，但对于理解数据库的基本运作原理已经相当有帮助了。<br />另外还有一些重要的概念没有实现还是比较遗憾的，比如平时常用到的**B+Tree索引，MVCC(多版本并发控制)**。希望以后有时间了，把这两个功能加上 :p

