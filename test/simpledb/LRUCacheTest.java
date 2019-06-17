package simpledb;

import org.junit.Test;
import org.omg.CORBA.INTERNAL;

import java.util.Iterator;

import static org.junit.Assert.*;

public class LRUCacheTest {

    @Test
    public void get() {
        LRUCache<Integer, Integer> cache = new LRUCache<>(1);
//        System.out.println(cache.put(1,2));
//        System.out.println(cache.get(1));
//        System.out.println(cache.put(2,3));
//        System.out.println(cache.get(1));
//        System.out.println(cache.get(2));

        assertNull(cache.put(1, 2));
        assertEquals(2L, (long) cache.get(1));
        assertEquals(2L, (long) cache.put(2, 3));
        assertNull(cache.get(1));
        assertEquals(3L, (long) cache.get(2));
    }

    @Test
    public void put() {
        LRUCache<Integer, Integer> cache = new LRUCache<>(2);
        cache.put(1, 2);
        cache.put(2, 2);
        cache.put(1, 2);
        cache.put(3, 2);
        cache.put(1, 2);
        cache.put(4, 2);
        cache.put(1, 2);
        cache.put(5, 2);
        cache.put(1, 2);
        cache.get(1);
        Iterator<Integer> iter = cache.iterator();
        while (iter.hasNext()) {
            iter.next();
        }
        System.out.println("aha");
    }
}