package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaskTwoMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final IntWritable counter = new IntWritable(0);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split(",");
		if(Utils.validRecord(tokens)){
			word.set(tokens[Utils.TAXI_ID_IDX]);
			if(!Utils.validGPS(tokens[Utils.PICK_LONG_IDX], tokens[Utils.PICK_LAT_IDX], 
									tokens[Utils.DROP_LONG_IDX], tokens[Utils.DROP_LAT_IDX])){
				counter.set(1);
			}
			context.write(word, counter);	
		} else {
			System.out.println("Invalid data record: " + value.toString());
		}
	}
}