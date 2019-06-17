package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * LRUCache实现
 */
public class LRUCache<K, V> {

    private DLNode head, tail;
    private int capacity;
    private Map<K, DLNode<K, V>> map = new HashMap<>();

    public LRUCache(int capacity) {
        this.capacity = capacity;
        head = new DLNode<>(null, null);
        tail = new DLNode<>(null, null);
        head.next = tail;
        tail.pre = head;
    }

    /**
     * @param key key
     * @return the value of the key, otherwise return null
     */
    @SuppressWarnings("unchecked")
    public V get(K key) {
        if (!map.containsKey(key)) return null;
        DLNode node = map.get(key);
        // connect pre and next
        removeNode(node);
        insertHead(node);
        return (V) node.value;
    }

    /**
     * @param k new key
     * @param v new value
     * @return if full, evicted value, else return null
     */
    @SuppressWarnings("unchecked")
    public V put(K k, V v) {
        V ret = null;
        DLNode node = null;
        if (map.containsKey(k)) {
            node = map.get(k);
            node.value = v;
            removeNode(node);
        } else {
            node = new DLNode(k, v);
            map.put(k, node);
        }
        insertHead(node);
        if (map.size() > capacity) {
            map.remove(tail.pre.key);
            ret = (V) removeNode(tail.pre).value;
        }
        return ret;
    }

    public boolean contains(K k) {
        return map.containsKey(k);
    }

    public int size() {
        return map.size();
    }

    private DLNode removeNode(DLNode node) {
        DLNode pre = node.pre, next = node.next;
        pre.next = next;
        next.pre = pre;

        return node;
    }

    private void insertHead(DLNode node) {
        DLNode hnext = head.next;
        head.next = node;
        node.pre = head;

        node.next = hnext;
        hnext.pre = node;
    }

    @Override
    public String toString() {
        return map.toString();
    }

    /**
     * 双向链表
     */
    static class DLNode<K, V> {
        K key;
        V value;
        DLNode pre, next;

        public DLNode(K k, V v) {
            key = k;
            value = v;
        }
    }

    /**
     * @return 当前缓存的所有value
     */
    public Iterator<V> iterator() {
        return new LruIter();
    }

    protected class LruIter implements Iterator<V> {
        DLNode n = head;

        @Override
        public synchronized boolean hasNext() {
            return n.next != tail;
        }

        @Override
        public synchronized V next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            n = n.next;
            return (V) n.value;
        }
    }

}
