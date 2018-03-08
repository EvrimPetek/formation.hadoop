package org.formation.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MapTime extends Mapper<Object, Text, Text, LongWritable> {
    private DateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    private Date date;
    private Text text = new Text();

    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(line.toString(), " []");
        text.set(st.nextToken().toString());
        if (st.hasMoreTokens()) {
            for (int i = 1; i < 3; i++) {
                st.nextToken();
            }
            if (st.hasMoreTokens()) {
                try {
                    date = df.parse(st.nextToken().toString());
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                context.write(text, new LongWritable(date.getTime()));
            }
        } else {
            System.out.println(line);
        }
    }
}