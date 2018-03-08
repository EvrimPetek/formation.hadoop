package org.formation.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GroupingComparator extends WritableComparator{

    public GroupingComparator() {
        super(PairInput.class,true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        PairInput p1=(PairInput) w1;
        PairInput p2= (PairInput) w2;

        return p1.getDoc_id().compareTo(p2.getDoc_id());
    }
}
