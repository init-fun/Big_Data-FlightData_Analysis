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
		//String Month = context.getConfiguration().get("Month");
	
		String s[] = value.toString().split(",");
		try{
			int n = Integer.parseInt(s[2]);
		
		
		
		
		if (n == 3 )
		{
			
			context.write(new Text(s[2]),new IntWritable(1));
		}
        
		}
		catch (Exception e)
		{
	System.out.println(e.getMessage());
		}
	}

}
