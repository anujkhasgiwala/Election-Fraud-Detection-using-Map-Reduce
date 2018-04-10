package ElectionFraud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FraudDetection {
	public static class FraudDetectionMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			IntWritable one = new IntWritable(1);

			word.set(value.toString().split("\t")[1] + " " + value.toString().split("\t")[2]+" 2006");
			context.write(word, one);
		}
	}
	
	public static class FraudDetectionMapper2 extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			IntWritable one = new IntWritable(1);

			word.set(value.toString().split("\t")[1] + " " + value.toString().split("\t")[2]+" 2008");
			context.write(word, one);
		}
	}

	public static class FraudDetectionReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			key.set(key.toString().split(" 200")[0]);
			context.write(key, result);
		}
	}
	
	public static class FraudDetectionMapper3 extends Mapper<Object, Text, Text, IntWritable> {

		@SuppressWarnings("unused")
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			IntWritable integer = new IntWritable(Integer.parseInt(values.toString()));

			word.set(values.toString());
			context.write((Text)key, integer);
		}
	}
	
	public static class FraudDetectionReducer2 extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			List<Double> countValues = new ArrayList<Double>();
			double percent = 0;
			for(int i = 0; i < 2; i++) {
				countValues.add(Double.parseDouble(values.iterator().next().toString()));
			}
			
			percent = ((countValues.get(1)-countValues.get(0))/countValues.get(0))*100;
			result.set((int)percent);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Fraud Detection");
		job.setJarByClass(FraudDetection.class);
		job.setMapperClass(FraudDetectionMapper.class);
		job.setReducerClass(FraudDetectionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FraudDetectionMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FraudDetectionMapper2.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		boolean jobSuccess = job.waitForCompletion(true);
		
		if(jobSuccess) {
			Job job1 = new Job(conf, "Fraud Detection");
			job1.setJarByClass(FraudDetection.class);
			job1.setMapperClass(FraudDetectionMapper3.class);
			job1.setReducerClass(FraudDetectionReducer2.class);
			job1.setInputFormatClass(KeyValueTextInputFormat.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job1, new Path(args[2]+"/part-r-00000"));
			FileOutputFormat.setOutputPath(job1, new Path("data/output1"));
			System.exit(job1.waitForCompletion(true) ? 0 : 1);
		}
	}
}