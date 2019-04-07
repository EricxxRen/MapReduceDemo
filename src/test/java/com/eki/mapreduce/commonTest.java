package com.eki.mapreduce;

import org.apache.hadoop.io.*;
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

    @Test
    public void testRegex () {
        String txt = "1,M,181,2003";
        String regex = "\\d+,[M,F],1[4-9]\\d,200[0-3]";
        System.out.println(txt);
        System.out.println(regex);
        if (txt.matches(regex)) {
            System.out.println(txt);
        }
    }

    @Test
    public void testString () {
        String txt = "2019/4/5,,mapreduce设计模式:BloomFilter,,N";
        String[] tokens = txt.split(",");
        System.out.println(tokens.length);
        for (String token : tokens) {
            if (token == null || token.length() == 0)
                System.out.println("it is blank");
            else
                System.out.println(token);
        }
    }

    @Test
    public void testSoredMapWritable () {
        SortedMapWritable test = new SortedMapWritable();
        test.put(new IntWritable(5), new Text("abc"));
        test.put(new IntWritable(2), new Text("efg"));
        System.out.println(test.lastKey());
    }
}
