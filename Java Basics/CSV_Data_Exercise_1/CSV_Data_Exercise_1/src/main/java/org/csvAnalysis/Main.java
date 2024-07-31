package org.csvAnalysis;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class Main {

    public static boolean HAS_HEADER = true;
    public static void main(String[] args) {
        String filePath = "/Users/aravindhan/Downloads/Birth and Death Dataset.csv";
        List<Record> records = GetRecordsFromCSV(filePath);
        DisplayAvailableRegions(records);
        DisplayAvailableRegions(records);
        DisplayOverallCounts(records);
        DisplayCountsInYear(records, 2015);
    }

    public static List<Record> GetRecordsFromCSV(String filePath) {
        List<Record> records = new ArrayList<>();
        String line;
        try (BufferedReader dataBuffer = new BufferedReader(new FileReader(filePath))) {
            if (HAS_HEADER) {
                dataBuffer.readLine();
            }
            while( (line = dataBuffer.readLine()) != null) {
                String[] splitData = line.split(",");
                Record record = new Record(Integer.parseInt(splitData[0]), splitData[1], splitData[2], Integer.parseInt(splitData[3]));
//                System.out.println(record);
                records.add(record);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Check the file path!");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return records;
    }

    public static void DisplayAvailableRegions(List<Record> records) {
        Set<String> uniqueRegions = new HashSet<String>();
        records.forEach(record -> {
            uniqueRegions.add(record.Region);
        });
        uniqueRegions.forEach(System.out::println);
    }

    public static void DisplayAvailablePeriod(List<Record> records) {
        Set<Integer> uniqueRegions = new HashSet<Integer>();
        records.forEach(record -> {
            uniqueRegions.add(record.Period);
        });
        uniqueRegions.forEach(System.out::println);
    }

    public static void DisplayOverallCounts(List<Record> records) {
        AtomicReference<Integer> birthCount = new AtomicReference<>(0);
        AtomicReference<Integer> deathCount = new AtomicReference<>(0);
        records.forEach(record -> {
            if(record.Type.equalsIgnoreCase("births")) {
                birthCount.updateAndGet(v -> v + record.Count);
            } else if (record.Type.equalsIgnoreCase("deaths")) {
                deathCount.updateAndGet(v -> v + record.Count);
            }
        });
        System.out.println("Birth Count for all year is: " + birthCount);
        System.out.println("Death Count for all year is: " + deathCount);
    }

    public static void DisplayCountsInYear(List<Record> records, Integer year) {
        AtomicReference<Integer> birthCount = new AtomicReference<>(0);
        AtomicReference<Integer> deathCount = new AtomicReference<>(0);
        records.forEach(record -> {
            if(record.Period.equals(year)) {
                if(record.Type.equalsIgnoreCase("births")) {
                    birthCount.updateAndGet(v -> v + record.Count);
                } else if (record.Type.equalsIgnoreCase("deaths")) {
                    deathCount.updateAndGet(v -> v + record.Count);
                }
            }
        });
        System.out.println("Birth Count for year " + year + " is: " + birthCount);
        System.out.println("Death Count for year " + year + " is: " + deathCount);
    }
}
