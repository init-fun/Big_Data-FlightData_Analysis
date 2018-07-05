package miet.cs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String Source_airport = context.getConfiguration().get("Source");
		  String info = value.toString();
		  String info_part [] = info.split(",");

		  String src = info_part[18];


		    if (src.equals(Source_airport))
		    {
		      IntWritable One = new IntWritable(1);
		      context.write(new Text(src), One);
	}

}
}
