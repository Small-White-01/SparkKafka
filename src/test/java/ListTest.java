import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ListTest {
    public static void main(String[] args) {
        int a=0;

        AtomicInteger count=new AtomicInteger();
        count.compareAndSet(a,a+2);
        System.out.println(count.get());
        HashMap<Integer,IntSort> map;


    }
}

class IntSort implements Comparable<IntSort>{

    private Integer key;
    private Integer val;
    private int rank;

    public IntSort(Integer key,Integer val, int rank) {
        this.key=key;
        this.val = val;
        this.rank = rank;
    }

    @Override
    public int compareTo(IntSort o) {
        if(rank>o.rank){return 1;
        }
        else if(rank<o.rank)return -1;
        return 0;
    }
}