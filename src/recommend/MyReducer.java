package recommend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String[] value;
        HashMap<String, Integer> commonMap = new HashMap<String, Integer>();
        
        for (Text val : values) {
            value = val.toString().split(",");
            if (value[0].equals("1")) {
                commonMap.put(value[1], -1);
            } else {
                if (commonMap.containsKey(value[1])) {
                    if (commonMap.get(value[1]) != -1) {
                        commonMap.put(value[1], commonMap.get(value[1]) + 1);
                    }
                } else {
                    commonMap.put(value[1], 1);
                }
            }
        }
        
        ArrayList<Entry<String, Integer>> commonList = new ArrayList<Entry<String, Integer>>();
        for (Entry<String, Integer> entry : commonMap.entrySet()) {
            if (entry.getValue() != -1) {
                commonList.add(entry);
            }
        }
        
        Collections.sort(commonList, new Comparator<Entry<String, Integer>>() {
            public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
                if (o1.getValue() == o2.getValue()) {
                    return 0;
                } else if (o1.getValue() > o2.getValue()) {
                    return -1;
                } else {
                    return 1;
                }
            }
        });
        
        final int MAX_RECOMMENDATION_COUNT = 10;
        List<String> top = new ArrayList<String>();
        for (int i = 0; i < Math.min(MAX_RECOMMENDATION_COUNT, commonList.size()); ++i) {
            top.add(commonList.get(i).getKey());
        }
        
        context.write(key, new Text(StringUtils.join(top, ",")));
    }
}
