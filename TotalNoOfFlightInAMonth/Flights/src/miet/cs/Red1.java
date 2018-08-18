package miet.cs;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Red1 extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
        Iterator vItt=values.iterator();
		  while(vItt.hasNext())
		  {
			  IntWritable i=(IntWritable)vItt.next();
			  sum=sum+i.get();
		  }
		  

		  

		  context.write(key,new IntWritable(sum));
	}

}
