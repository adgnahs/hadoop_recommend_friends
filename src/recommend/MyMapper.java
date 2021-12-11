package recommend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] userAndFriends = line.split("t");
        if (userAndFriends.length != 2) {
            return;
        }
        
        IntWritable userId = new IntWritable(Integer.parseInt(userAndFriends[0]));
        List<IntWritable> friends = new ArrayList<IntWritable>();
        for (String item : userAndFriends[1].split(",")) {
            friends.add(new IntWritable(Integer.parseInt(item)));
        }
        
        int index = 0;
        for (IntWritable friend1 : friends) {
            context.write(userId, new Text("1," + friend1));
            ++index;
            
            for (IntWritable friend2 : friends.subList(index, friends.size())) {
                context.write(friend1, new Text("2," + friend2));
                context.write(friend2, new Text("2," + friend1));
            }
        } 
    }
}
