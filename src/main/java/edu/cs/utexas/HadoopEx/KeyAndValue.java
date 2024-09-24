package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public class KeyAndValue implements Comparable<KeyAndValue> {

        private final Text key;
        private final FloatWritable value;

        public KeyAndValue(Text key, FloatWritable value) {
            this.key = key;
            this.value = value;
        }

        public Text getKey() {
            return key;
        }

        public FloatWritable getValue() {
            return value;
        }
   
        public int compareTo(KeyAndValue other) {
            return Float.compare(getValue().get(), other.getValue().get());
        }


        public String toString(){

            return "(" + key.toString() + " , " + value.toString() + ")";
        }
    }
