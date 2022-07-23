import lombok.extern.log4j.Log4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
@Log4j
class LRUCache {

    public static void main(String[] args) throws InterruptedException {

        LRUCache lRUCache = new LRUCache(2);
        lRUCache.put(1, 1); // 缓存是 {1=1}
        lRUCache.put(2, 2); // 缓存是 {1=1, 2=2}
        lRUCache.get(1);    // 返回 1
        lRUCache.put(3, 3); // 该操作会使得关键字 2 作废，缓存是 {1=1, 3=3}
        lRUCache.get(2);    // 返回 -1 (未找到)
        lRUCache.put(4, 4); // 该操作会使得关键字 1 作废，缓存是 {4=4, 3=3}
        lRUCache.get(1);    // 返回 -1 (未找到)
        lRUCache.get(3);    // 返回 3
        lRUCache.get(4);    // 返回 4

    }

    Map<Integer,IntSort> map;
    int capacity;
    AtomicLong timeStamp=new AtomicLong();
    public LRUCache(int capacity) {
        map=new HashMap<Integer,IntSort>(capacity);
        this.capacity=capacity;
    }
    
    public int get(int key) throws InterruptedException {

        long l = toNextMs();
            if (map.containsKey(key)) {
                IntSort intSort = map.get(key);
                intSort.ts = l;
                log.info(intSort.val + "\t");
                log.info(intSort + "\n");
                return intSort.val;
            }
            log.info(-1 + "\n");
            return -1;

    }
    
    public void put(int key, int value) {
        long l = toNextMs();
        if(map.size()==this.capacity){
                IntSort min = Collections.min(map.values());
                Integer k=min.key;
                map.remove(k);
            }

            IntSort intSort = new IntSort(key, value,l);
            map.put(key, intSort);
            log.info("null"+"\t");
            log.info(intSort+"\n");



    }
    public long toNextMs(){
        long ts =0;
        while ((ts==System.currentTimeMillis())&&timeStamp.compareAndSet(ts,System.currentTimeMillis())){

        }

        return ts;
    }
    static class IntSort implements Comparable<IntSort>{

        Integer key;
        Integer val;
        long ts;

        public IntSort(Integer key,Integer val, long ts) {
            this.key=key;
            this.val = val;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return "IntSort{" +
                    "key=" + key +
                    ", val=" + val +
                    ", ts=" + ts +
                    '}';
        }

        @Override
        public int compareTo(IntSort o) {
            if(ts>o.ts){return 1;
            }
            else if(ts<o.ts)return -1;
            return 0;
        }
    }
}


/**
 * Your LRUCache object will be instantiated and called as such:
 * LRUCache obj = new LRUCache(capacity);
 * int param_1 = obj.get(key);
 * obj.put(key,value);
 */