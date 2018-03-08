package org.formation.mapreduce;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapFiltreTest {
    MapDriver<LongWritable,Text,PairInput,IntWritable> mapDriver;
    @Before
    public void setUp() {
        MapFiltre mapper= new MapFiltre();
        mapDriver=MapDriver.newMapDriver(mapper);
}
    @Test
    public void testMapper() throws IOException, URISyntaxException {
        //mapDriver.setCounters(new Counters());
        //mapDriver.addCacheFile(new URI("C:\\Users\\diginamic\\Downloads\\tpMapReduce\\tp2\\tp2-mp\\stopwords_en.txt"));
        //mapDriver.withInput(new LongWritable(),new Text("aback '\"!?,.()[]+-_0123456789<>;:"));
        //mapDriver.withOutput(new PairInput("aback","somefile"),new IntWritable(1));
        //mapDriver.withAllOutput(List<Pair<PairInput,IntWritable>>);
        //mapDriver.runTest();
    }

}
