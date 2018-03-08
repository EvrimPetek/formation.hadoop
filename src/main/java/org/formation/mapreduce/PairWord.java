package org.formation.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWord implements WritableComparable<PairWord> {

    private IntWritable mot;
    private IntWritable doc;

    public PairWord(IntWritable mot,IntWritable doc) {
        this.mot=mot;
        this.doc=doc;
    }

    public PairWord(int mot, int doc) {
        this.mot=new IntWritable(mot);
        this.doc=new IntWritable(doc);

    }
    public PairWord() {
        this.mot=new IntWritable();
        this.doc=new IntWritable();
    }

    public IntWritable getMot() { return mot; }

    public void setMot(IntWritable mot) { this.mot = mot;    }

    public IntWritable getDoc() { return doc;    }

    public void setDoc(IntWritable doc) { this.doc = doc; }

    public void write(DataOutput out) throws IOException {
        mot.write(out);
        doc.write(out);
    }
    public void readFields(DataInput in) throws IOException {
        mot.readFields(in);
        doc.readFields(in);
    }
    public String toString() {
        return mot.toString() + " " + doc.toString();
    }
    public int hashCode() {
        return mot.hashCode()*163 + doc.hashCode();
    }

    public int compareTo(org.formation.mapreduce.PairWord o) {
        if(mot.get() == o.getMot().get())
            return 0;
        else  if(mot.get() > o.getMot().get())
            return 1;
        else return -1;
    }
}
