package org.formation.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerPair  extends Partitioner<PairInput,IntWritable> {


    public int getPartition(PairInput pairInput, IntWritable intWritable, int i) {
        if(pairInput.getDoc_id().toString().equals("mobydick.txt"))
            return 0;
        if(pairInput.getDoc_id().toString().equals("r_crusoe"))
            return 1;
        return 0;
    }
}
