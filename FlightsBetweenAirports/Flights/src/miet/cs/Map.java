package miet.cs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String Source_airport = context.getConfiguration().get("Origin");
		String Dest_airport = context.getConfiguration().get("Dest");
		String s[] = value.toString().split(",");
		if (s[17].equals(Source_airport) && s[18].equals(Dest_airport))
			 context.write(new Text(s[11]),new IntWritable(1));
		
        
		
	}

}
