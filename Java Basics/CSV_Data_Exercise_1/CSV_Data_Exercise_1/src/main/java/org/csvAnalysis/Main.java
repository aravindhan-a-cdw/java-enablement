package org.csvAnalysis;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class Main {

    public static boolean HAS_HEADER = true;
    public static void main(String[] args) {
        String filePath = "/Users/aravindhan/Downloads/Birth and Death Dataset.csv";
        HashMap<String, RegionCollection> recordsCollection = GetRecordsFromCSV(filePath);
        DisplayAvailableRegions(recordsCollection);
        DisplayAvailablePeriod(recordsCollection);
        DisplayOverallCounts(recordsCollection);
        DisplayCountsInYear(recordsCollection, 2015);
        DisplayCountsByRegion(recordsCollection, "Northland region");
        var highestBirthCount = FindYearWithHighestCount(recordsCollection, "Births");
        var highestDeathCount = FindYearWithHighestCount(recordsCollection, "Deaths");
        System.out.println("The year with highest births and deaths are " + highestBirthCount.Count + " and " + highestDeathCount.Count);
        FindHighestCountInEachRegion(recordsCollection);
    }

    public static HashMap<String, RegionCollection> GetRecordsFromCSV(String filePath) {
        var line = "";
        HashMap<String, RegionCollection> regionCollectionHashMap = new HashMap<>();
        try (BufferedReader dataBuffer = new BufferedReader(new FileReader(filePath))) {
            if (HAS_HEADER) {
                dataBuffer.readLine();
            }
            while( (line = dataBuffer.readLine()) != null) {
                String[] splitData = line.split(",");
                Record record = new Record(Integer.parseInt(splitData[0]), splitData[1], splitData[2], Integer.parseInt(splitData[3]));
                if(!regionCollectionHashMap.containsKey(record.Region)){
                    var regionCollection = new RegionCollection(record.Region);
                    regionCollectionHashMap.put(record.Region, regionCollection);
                }
                var regionCollection = regionCollectionHashMap.get(record.Region);
                try {
                    regionCollection.AddRecord(record);
                } catch (Exception e) {
                    System.out.println("Duplicate record found");
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("Check the file path!");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return regionCollectionHashMap;
    }

    public static void DisplayAvailableRegions(HashMap<String, RegionCollection> records) {
        System.out.println("All Available Regions are:");
        System.out.println("--------------------------");
        records.forEach((region, regionCollection)-> {
            System.out.println(region);
        });
        System.out.println("--------------------------");
    }

    public static void DisplayAvailablePeriod(HashMap<String, RegionCollection> records) {
        Set<Integer> uniquePeriods = new HashSet<Integer>();
        records.forEach((region, regionCollection) -> {
            regionCollection.records.forEach(record -> {
                uniquePeriods.add(record.Period);
            });
        });
        System.out.println("All Available Periods are:");
        System.out.println("--------------------------");
        uniquePeriods.forEach(System.out::println);
        System.out.println("--------------------------");
    }

    public static void DisplayOverallCounts(HashMap<String, RegionCollection> records) {
        AtomicReference<Integer> birthCount = new AtomicReference<>(0);
        AtomicReference<Integer> deathCount = new AtomicReference<>(0);
        records.forEach((region, regionCollection) -> {
            regionCollection.records.forEach(record -> {
                if(record.Type.equalsIgnoreCase("births")) {
                    birthCount.updateAndGet(v -> v + record.Count);
                } else {
                    deathCount.updateAndGet(v -> v + record.Count);
                }
            });
        });
        System.out.println("--------------------------");
        System.out.println("Birth Count for all year is: " + birthCount);
        System.out.println("Death Count for all year is: " + deathCount);
        System.out.println("--------------------------");
    }

    public static void DisplayCountsInYear(HashMap<String, RegionCollection> records, Integer year) {
        AtomicReference<Integer> birthCount = new AtomicReference<>(0);
        AtomicReference<Integer> deathCount = new AtomicReference<>(0);
        records.forEach((region, regionCollection) -> {
            regionCollection.records.forEach(record -> {
                if(record.Period != year) return;
                if(record.Type.equalsIgnoreCase("births")) {
                    birthCount.updateAndGet(v -> v + record.Count);
                } else {
                    deathCount.updateAndGet(v -> v + record.Count);
                }
            });
        });
        System.out.println("--------------------------");
        System.out.println("Birth Count for year " + year + " is: " + birthCount);
        System.out.println("Death Count for year " + year + " is: " + deathCount);
        System.out.println("--------------------------");
    }

    public static void DisplayCountsByRegion(HashMap<String, RegionCollection> records, String region) {
        AtomicReference<Integer> birthCount = new AtomicReference<>(0);
        AtomicReference<Integer> deathCount = new AtomicReference<>(0);
        var regionCollection = records.get(region);
        System.out.println("--------------------------");
        System.out.println("Birth Count for region " + region + " is: " + regionCollection.Counts.get("Births"));
        System.out.println("Death Count for region " + region + " is: " + regionCollection.Counts.get("Deaths"));
        System.out.println("--------------------------");
    }

    public static Record FindYearWithHighestCount(HashMap<String, RegionCollection> records, String type) {
        AtomicReference<Record> selectedRecord = new AtomicReference<>();
        records.forEach((region, regionCollection) -> {
            var currentRecord = regionCollection.MaxCountRecord.get(type);
            if(selectedRecord.get() == null) {
                selectedRecord.set(currentRecord);
                return;
            }
            if(selectedRecord.get().Count < currentRecord.Count) {
                selectedRecord.set(currentRecord);
            }
        });
        return selectedRecord.get();
    }

    public static void FindHighestCountInEachRegion(HashMap<String, RegionCollection> records) {
        System.out.println("--------------------------");
        records.forEach((region, regionCollection) -> {
            var birthRecord = regionCollection.MaxCountRecord.get("Births");
            System.out.println("The region " + region + " has highest births of " + birthRecord.Count + " in year " + birthRecord.Period);
            var deathRecord = regionCollection.MaxCountRecord.get("Deaths");
            System.out.println("The region " + region + " has highest deaths of " + deathRecord.Count + " in year " + deathRecord.Period);

        });
        System.out.println("--------------------------");
    }
}
