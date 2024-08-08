package org.csvAnalysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RegionCollection {
    String Region;
    HashMap<String, Record> MaxCountRecord;
    HashMap<String, Integer> Counts;
    List<Record> records;
    public RegionCollection(String region) {
        this.Region = region;
        records = new ArrayList<>();
        MaxCountRecord = new HashMap<>();
        Counts = new HashMap<>();
    }
    public void AddRecord(Record record) throws Exception {
        if(!record.Region.equalsIgnoreCase(this.Region)) {
            throw new Exception("Region doesn't match for the record");
        }
        this.records.add(record);
        if(MaxCountRecord.containsKey(record.Type)) {
            var currentMaxRecord = MaxCountRecord.get(record.Type);
            if(currentMaxRecord.Count < record.Count) {
                MaxCountRecord.put(record.Type, record);
            }
        } else {
            MaxCountRecord.put(record.Type, record);
        }
        if(Counts.containsKey(record.Type)){
            Counts.put(record.Type, Counts.get(record.Type) + record.Count);
        } else {
            Counts.put(record.Type, record.Count);
        }
    }
    public Record GetMaxRecord(String type){
        return MaxCountRecord.get(type);
    }
}
