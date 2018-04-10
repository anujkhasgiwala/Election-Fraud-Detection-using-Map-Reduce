package ElectionFraud;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MonolithicCounty2006 {
	public static class MonolithicCounty2006Mapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			IntWritable one = new IntWritable(1);

			word.set(value.toString().split("\t")[1] + " " + value.toString().split("\t")[2]);
			context.write(word, one);
		}
	}   

	public static class MonolithicCounty2006Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	//There is no requirements for 2 jobs
	/*public static class MonolithicCounty2006Mapper2 extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			IntWritable one = new IntWritable(1);

			word.set(value);
			context.write(word, one);
		}
	}   

	public static class MonolithicCounty2006Reducer2 extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}*/

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Monolithic County 2006");
		job.setJarByClass(MonolithicCounty2006.class);
		job.setMapperClass(MonolithicCounty2006Mapper.class);
		job.setReducerClass(MonolithicCounty2006Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		/*boolean job1Success = job1.waitForCompletion(true);

		if(job1Success) {
			Job job2 = Job.getInstance(conf, "Monolithic County 2006 JOB_2");
		    job2.setMapperClass(MonolithicCounty2006Mapper2.class);
		    job2.setReducerClass(MonolithicCounty2006Reducer2.class);
		    //job2.setInputFormatClass(KeyValueTextInputFormat.class);
		    FileInputFormat.addInputPath(job2, new Path(args[2]));
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}*/
	}
}
