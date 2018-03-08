package org.formation.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;

public class ReduceTfIdf extends Reducer<PairInput,IntWritable,PairInput,PairWord> {
    private IntWritable result = new IntWritable();
    private HashMap<PairInput,Integer> map=new HashMap<PairInput,Integer>();
    private PairWord pairmot= new PairWord();
    @Override
    protected void reduce(PairInput key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        int sum = 0;
        for (IntWritable val : values) {
            System.out.println(key.getMot().toString());
            PairInput auxKey = new PairInput(new Text(key.getMot().toString()),new Text(key.getDoc_id()));
            map.put(auxKey,val.get());
            sum += val.get();
        }
        for (Entry<PairInput,Integer> entry: map.entrySet()) {

                pairmot.setMot(new IntWritable(entry.getValue()));
                pairmot.setDoc(new IntWritable(sum));
                context.write(entry.getKey(),pairmot);

        }
        map.clear();
        //context.write(key,pairmot);
        //map.put(key,sum);


        //result.set(sum);
        //context.write(key, result);
    }
}
