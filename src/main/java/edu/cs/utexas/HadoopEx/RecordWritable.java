package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecordWritable implements Writable {
    private FloatWritable earnings;
    private FloatWritable duration;

    public RecordWritable() {
        this.earnings = new FloatWritable();
        this.duration = new FloatWritable();
    }
    
    public RecordWritable(FloatWritable earnings, FloatWritable duration) {
        this.earnings = earnings;
        this.duration = duration;
    }

    public FloatWritable getEarnings() {
        return earnings;
    }

    public FloatWritable getDuration() {
        return duration;
    }

    public void write(DataOutput out) throws IOException {
        earnings.write(out);
        duration.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        earnings.readFields(in);
        duration.readFields(in);
    }
}
