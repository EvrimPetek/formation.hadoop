package org.formation.mapreduce;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceTime extends Reducer<Text,/*IntWritable*/LongWritable,Text,/*IntWritable*/LongWritable> {
    //private IntWritable resultat = new IntWritable();
    private LongWritable duree = new LongWritable(0);

    public void reduce(Text key, Iterable<LongWritable> valeurs, Context context) throws IOException, InterruptedException {
        LongWritable dateMin = new LongWritable(0);
        LongWritable dateMax = new LongWritable(0);

        for (LongWritable date : valeurs) {
            if(date.get() < dateMin.get()) {
                dateMin.set(date.get());
            }
            if(date.get() > dateMax.get()) {
                dateMax.set(date.get());
            }
        }
        long dif = dateMax.get() - dateMin.get();
        duree.set(dif);
        context.write(key,duree);
    }
}