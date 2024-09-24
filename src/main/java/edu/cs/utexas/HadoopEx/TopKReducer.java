package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;



public class TopKReducer extends  Reducer<Text, FloatWritable, Text, FloatWritable> {

    private PriorityQueue<KeyAndValue> pq = new PriorityQueue<KeyAndValue>(10);;

   public void reduce(Text key, Iterable<FloatWritable> values, Context context)
           throws IOException, InterruptedException {

       // Size of values is 1 because key only has one distinct value
       for (FloatWritable value : values) {
           pq.add(new KeyAndValue(new Text(key), new FloatWritable(value.get())));
       }

       while (pq.size() > Utils.K) {
           pq.poll();
       }


   }

    public void cleanup(Context context) throws IOException, InterruptedException {
        List<KeyAndValue> values = new ArrayList<KeyAndValue>(10);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        // Print the Top K by reversing the PQ
        Collections.reverse(values);
        for (KeyAndValue value : values) {
            context.write(value.getKey(), value.getValue());
        }
    }
}