package miet.cs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] programArgs= new GenericOptionsParser(conf,args).getRemainingArgs();
		conf.set("Source", programArgs[2]);
		// Use programArgs array to retrieve program arguments.
		 //Airport to search
		    Job job = new Job(conf, "Number of flights Job");
		    job.setJarByClass(Driver.class);
		    job.setMapperClass(Mapper.class);
		    job.setReducerClass(Reducer.class);
		    //change the data type as needed by the program
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(programArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(programArgs[1]));

		    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
