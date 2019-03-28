package com.eki.mapreduce;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
}
