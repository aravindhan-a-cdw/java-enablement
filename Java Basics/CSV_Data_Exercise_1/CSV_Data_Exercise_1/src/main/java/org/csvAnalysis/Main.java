package org.csvAnalysis;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class Main {

    public static boolean HAS_HEADER = true;
    public static void main(String[] args) {
        String filePath = "/Users/aravindhan/Downloads/Birth and Death Dataset.csv";
        List<Record> records = GetRecordsFromCSV(filePath);
        DisplayAvailableRegions(records);
        DisplayAvailablePeriod(records);
        DisplayOverallCounts(records);
        DisplayCountsInYear(records, 2015);
        DisplayCountsByRegion(records, "Northland region");
        var highestBirthCount = FindYearWithHighestCount(records, "births");
        var highestDeathCount = FindYearWithHighestCount(records, "deaths");
        System.out.println("The year with highest births and deaths are " + highestBirthCount + " and " + highestDeathCount);
        FindHighestCountInEachRegion(records);
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

    public static void DisplayCountsByRegion(List<Record> records, String region) {
        AtomicReference<Integer> birthCount = new AtomicReference<>(0);
        AtomicReference<Integer> deathCount = new AtomicReference<>(0);
        records.forEach(record -> {
            if(record.Region.equalsIgnoreCase(region)) {
                if(record.Type.equalsIgnoreCase("births")) {
                    birthCount.updateAndGet(v -> v + record.Count);
                } else if (record.Type.equalsIgnoreCase("deaths")) {
                    deathCount.updateAndGet(v -> v + record.Count);
                }
            }
        });
        System.out.println("Birth Count for region " + region + " is: " + birthCount);
        System.out.println("Death Count for region " + region + " is: " + deathCount);
    }

    public static Record FindYearWithHighestCount(List<Record> records, String type) {
        AtomicReference<Record> selectedRecord = new AtomicReference<>(records.getFirst());
        records.forEach(record -> {
            if(record.Count > selectedRecord.get().Count) {
                selectedRecord.set(record);
            }
        });
        return selectedRecord.get();
    }

    public static void FindHighestCountInEachRegion(List<Record> records) {
        var regionWiseRecords = new HashMap<String, HashMap<String, Record>>();
        records.forEach(record -> {
            if(!regionWiseRecords.containsKey(record.Region)){
                HashMap<String, Record> recordMap = new HashMap<String, Record>();
                recordMap.put("Births", null);
                recordMap.put("Deaths", null);
                recordMap.put(record.Type, record);
                regionWiseRecords.put(record.Region, recordMap);
            }
            var currentRegionRecords = regionWiseRecords.get(record.Region);
            if(currentRegionRecords.get(record.Type) == null) {
                currentRegionRecords.put(record.Type, record);
                return;
            }
            if(record.Count > currentRegionRecords.get(record.Type).Count) {
                currentRegionRecords.put(record.Type, record);
            }
        });
        regionWiseRecords.forEach((region, recordOfRegion) -> {
            var birthRecord = recordOfRegion.get("Births");
            if(birthRecord != null) {
                System.out.println("The region " + region + " has highest births of " + birthRecord.Count + " in year " + birthRecord.Period);
            }
            var deathRecord = recordOfRegion.get("Deaths");
            if(deathRecord != null) {
                System.out.println("The region " + region + " has highest deaths of " + deathRecord.Count + " in year " + deathRecord.Period);
            }
        });
    }
}
