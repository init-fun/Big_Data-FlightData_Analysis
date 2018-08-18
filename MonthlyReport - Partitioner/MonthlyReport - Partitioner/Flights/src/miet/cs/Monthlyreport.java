package miet.cs;


import java.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class Monthlyreport extends Configured implements Tool
{
	//Map class
	public static class Mapclass extends
	Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context)
		{
			try
			{
	String[]str=value.toString().split(",");
	String dmonth=str[11];
	context.write(new Text(dmonth),new Text(value));
			}
		catch (Exception e)
		{
	System.out.println(e.getMessage());
		}
		}
	}
	//Reducer class
	public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
	
	{
	
	public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException
	{
		int num=0;
		for(Text val:values)
		{
			num = num +1;
		}
		context.write(new Text(key),new IntWritable(num));
	}
	}
	//Partioner class
	public static class caderPartitioner extends
	Partitioner <Text,Text>
	{
		@Override
		public int getPartition(Text key,Text value,int numReduceTasks)
		{
			String[]str=value.toString().split(",");
	int m=Integer.parseInt(str[2]);
	 
	if(numReduceTasks<1)
	{
		return 0;
	}
	
	
	     if(m==1)
		
		{
			return 0;
		}else if(m==2){
			return  1 % numReduceTasks;
		}else if(m==3){
			return  2 % numReduceTasks;
		}else if(m==4){
			return  3 % numReduceTasks;
		}else if(m==5){
			return  4 % numReduceTasks;
		}else{
			return  5 % numReduceTasks;
		}
			
	}
}
@Override
public int run(String[] args) throws Exception
{
	Configuration conf=getConf();
	Job job=new Job(conf,"Month");
	job.setJarByClass(Monthlyreport.class);
	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setMapperClass(Mapclass.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
	job.setPartitionerClass(caderPartitioner.class);
	job.setNumReduceTasks(6);
	job.setReducerClass(ReduceClass.class);
	job.setInputFormatClass(TextInputFormat.class);
	 job.setOutputFormatClass(TextOutputFormat.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(Text.class);
     System.exit(job.waitForCompletion(true)?0:1);
  return 0;
}
public static void main(String ar[]) throws Exception
		{
	int res=ToolRunner.run(new Configuration(),new Monthlyreport(),ar);
	
	System.exit(0);
}
}
	
