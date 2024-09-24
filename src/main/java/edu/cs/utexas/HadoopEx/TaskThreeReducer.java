package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaskThreeReducer extends  Reducer<Text, RecordWritable, Text, FloatWritable> {

   public void reduce(Text text, Iterable<RecordWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       float totalEarnings = 0;
       float totalDuration = 0;

       for (RecordWritable value : values) {
            totalEarnings += ((FloatWritable) value.getEarnings()).get();
            totalDuration += ((FloatWritable) value.getDuration()).get();
       }
       
       context.write(text, new FloatWritable((totalEarnings / totalDuration) * 60));
   }
}