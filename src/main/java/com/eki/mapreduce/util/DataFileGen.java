package com.eki.mapreduce.util;

import java.io.File;
import java.io.FileWriter;
import java.util.Random;

public class DataFileGen {
    public static void main(String[] args) {
        File file = new File ("/home/xiaoxing/IdeaProjects/MapReduce/mmc_input/info.csv");

        try {
            Random random = new Random();
            FileWriter fileWriter = new FileWriter(file);

            for (int i = 1; i <= 100000; i++) {
                String gender = getGender();
                String year = getYear();

                if ("M".equals(gender)) {
//                    double hight = 180D + random.nextGaussian() * 15D;
                    double hight = 180D + random.nextGaussian() * Math.sqrt(15D);
                    fileWriter.write(i + "," + gender + "," + (int) hight + "," + year);
                    fileWriter.write(System.getProperty("line.separator"));
                } else {
//                    double hight = 160D + random.nextGaussian() * 15D;
                    double hight = 160D + random.nextGaussian() * Math.sqrt(15D);
                    fileWriter.write(i + "," + gender + "," + (int) hight + "," + year);
                    fileWriter.write(System.getProperty("line.separator"));
                }

            }
            fileWriter.flush();
            fileWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getGender () {
        Random random = new Random();
        int rNum = random.nextInt(2) + 1;
        if (rNum % 2 == 0) {
            return "M";
        } else {
            return "F";
        }
    }

    public static String getYear () {
        Random random = new Random();
        Integer year = random.nextInt(5) + 2000;
        return year.toString();
    }
}
