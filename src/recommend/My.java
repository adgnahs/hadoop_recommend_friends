package recommend;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.*;

import java.util.Scanner;

public class My {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		/*Scanner input=new Scanner(System.in);
		String path=input.next();
		input.close();*/
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:9820");
		conf.set("mapreduce.framework.name", "local");
		conf.set("dfs.replication", "1");
		conf.set("dfs.blocksize", "1m");
		Job job = Job.getInstance(conf, "MoreFileDriver");
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path("/test"));
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		Path outPath = new Path("/result");
		if(outPath.getFileSystem(conf).exists(outPath)) 
			outPath.getFileSystem(conf).delete(outPath,true);
		TextOutputFormat.setOutputPath(job, outPath);
		job.waitForCompletion(true);
	}
}