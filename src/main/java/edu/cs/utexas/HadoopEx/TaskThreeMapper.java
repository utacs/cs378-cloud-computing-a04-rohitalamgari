package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaskThreeMapper extends Mapper<Object, Text, Text, RecordWritable> {

    private final FloatWritable earnings = new FloatWritable(0);
    private final FloatWritable duration = new FloatWritable(0);


	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split(",");
		if(Utils.validRecord(tokens)){
			word.set(tokens[Utils.DRIVER_ID_IDX]);
            earnings.set(Float.parseFloat(tokens[Utils.TOTAL_AMT_IDX]));
            duration.set(Float.parseFloat(tokens[Utils.TRIP_DUR_IDX]));

			context.write(word, new RecordWritable(earnings, duration));	
		} else {
			System.out.println("Invalid data record: " + value.toString());
		}
	}
}