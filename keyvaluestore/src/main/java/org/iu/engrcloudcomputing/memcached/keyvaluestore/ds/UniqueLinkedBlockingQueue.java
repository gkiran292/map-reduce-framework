package org.iu.engrcloudcomputing.memcached.keyvaluestore.ds;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class UniqueLinkedBlockingQueue<E> extends LinkedBlockingQueue<E> {
    private Set<E> set = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    public synchronized void put(E e) throws InterruptedException {
        if (set.contains(e)) {
            return;
        }
        set.add(e);
        super.put(e);
    }

    @Override
    public synchronized boolean offer(E e) {
        if (set.contains(e)) {
            return false;
        }
        set.add(e);
        return super.offer(e);
    }

    /**
     * Retrieves and removes the head of this queue, waiting if necessary until an element becomes available.
     * No synchronization is needed here, as we don't care about the element staying in the set longer than needed.
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public E take() throws InterruptedException {
        E head = super.take();
        set.remove(head);
        return head;
    }
}