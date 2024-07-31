package org.csvAnalysis;

import java.util.Objects;

public class Record implements Comparable {
    public Integer Period;
    public String Type;
    public String Region;
    public Integer Count;

    public Record(Integer period, String type, String region, Integer count) {
        this.Period = period;
        this.Type = type;
        this.Region = region;
        this.Count = count;
    }


    @Override
    public int compareTo(Object o) {
        if(o instanceof Record) {
            if(Objects.equals(((Record) o).Period, this.Period)
                    && Objects.equals(((Record) o).Count, this.Count)
                    && Objects.equals(((Record) o).Region, this.Region)
                    && Objects.equals(((Record) o).Type, this.Type)) return 0;
            if(((Record) o).Count > this.Count) return 1;
            return -1;
        }
        return 0;
    }
}
