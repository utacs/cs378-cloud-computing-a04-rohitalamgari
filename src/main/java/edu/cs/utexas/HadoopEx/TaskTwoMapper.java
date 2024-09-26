package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaskTwoMapper extends Mapper<Object, Text, Text, IntWritable> {


	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		Text word = new Text();
		IntWritable counter = new IntWritable(0);
		
		String[] tokens = value.toString().split(",");
		if(Utils.validRecord(tokens)){
			word.set(tokens[Utils.TAXI_ID_IDX]);
			
			if(!Utils.isValidCord(tokens[Utils.PICK_LONG_IDX], tokens[Utils.PICK_LAT_IDX]) || 
			   !Utils.isValidCord(tokens[Utils.DROP_LONG_IDX], tokens[Utils.DROP_LAT_IDX])) {
				counter.set(1);
			}

			context.write(word, counter);	
		} else {
			System.out.println("Invalid data record: " + value.toString());
		}
	}
}