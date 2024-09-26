package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;

public class TopKMapper extends Mapper<Text, Text, Text, FloatWritable> {
	private PriorityQueue<KeyAndValue> pq = new PriorityQueue<>();

	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {


		float count = Float.parseFloat(value.toString());

		pq.add(new KeyAndValue(new Text(key), new FloatWritable(count)) );

		if (pq.size() > 10) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		while (pq.size() > 0) {
			KeyAndValue wordAndCount = pq.poll();
			context.write(wordAndCount.getKey(), wordAndCount.getValue());
		}
	}
}