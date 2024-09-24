package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaskTwoReducer extends  Reducer<Text, IntWritable, Text, FloatWritable> {

   public void reduce(Text text, Iterable<IntWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       int sum = 0;
       int counter = 0;

       for (IntWritable value : values) {
           sum += value.get();
           counter++;
       }
       
       context.write(text, new FloatWritable((float) sum / counter));
   }
}