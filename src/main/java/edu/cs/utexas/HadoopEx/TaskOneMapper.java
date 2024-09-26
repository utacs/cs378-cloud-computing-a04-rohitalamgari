package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaskOneMapper extends Mapper<Object, Text, Text, IntWritable> {

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		Text pickTime = new Text();
		Text dropTime = new Text();

		IntWritable pickError = new IntWritable(0);
		IntWritable dropError = new IntWritable(0);

		String[] tokens = value.toString().split(",");
		if(Utils.validRecord(tokens)){
			pickTime.set(Utils.extractHour(tokens[Utils.PICK_TIME_IDX]));
			dropTime.set(Utils.extractHour(tokens[Utils.DROP_TIME_IDX]));
			
			if(!Utils.isValidCord(tokens[Utils.PICK_LONG_IDX], tokens[Utils.PICK_LAT_IDX])) {
				pickError.set(1);
			}

			if(!Utils.isValidCord(tokens[Utils.DROP_LONG_IDX], tokens[Utils.DROP_LAT_IDX])) {
				dropError.set(1);
			}

			context.write(pickTime, pickError);	
			context.write(dropTime, dropError);	
		} else {
			System.out.println("Invalid data record: " + value.toString());
		}
	}
}