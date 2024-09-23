package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaskOneMapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create a counter and initialize with 1
	private IntWritable counter = new IntWritable(0);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split(",");
		if(Utils.validRecord(tokens)){
			word.set(Utils.extractHour(tokens[2]));
			if(!Utils.validGPS(tokens[6], tokens[7], tokens[8], tokens[9])){
				counter.set(1);
			}
			context.write(word, counter);	
		}

		// StringTokenizer itr = new StringTokenizer(value.toString());
		// while (itr.hasMoreTokens()) {
		// 	word.set(itr.nextToken());
		// 	context.write(word, counter);
		// }
	}
}