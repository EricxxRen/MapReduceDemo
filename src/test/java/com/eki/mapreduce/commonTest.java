package com.eki.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

import java.util.*;

public class commonTest {

    @Test
    public void commontest () {
        List<String> stringList = new ArrayList<String>();
        for (String string : stringList) {
            System.out.println(string);
        }
    }

    @Test
    public void testGaussian () {
        Random random = new Random();
        for (int i = 0; i < 50; i++) {
            System.out.println(random.nextGaussian());
        }
    }

    @Test
    public void testMultiply () {
        float a = 0f;
        int b = 2;
        long c = 3;
        a = b * c;
        System.out.println(a);
    }

    @Test
    public void testSortedMap () {
        SortedMapWritable sortedMapWritable = new SortedMapWritable();
        sortedMapWritable.put(new IntWritable(1), new IntWritable(1));
        Set<Map.Entry<WritableComparable, Writable>> entries = sortedMapWritable.entrySet();
        for (Map.Entry entry : entries) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

        sortedMapWritable.put(new IntWritable(2), new IntWritable(2));
        Set<Map.Entry<WritableComparable, Writable>> entries2 = sortedMapWritable.entrySet();
        for (Map.Entry entry : entries2) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}
