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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ElectionFraud.ElectionWinner2008.ElectionWinner2008Mapper;
import ElectionFraud.ElectionWinner2008.ElectionWinner2008Reducer;
import ElectionFraud.FraudDetection.FraudDetectionMapper;
import ElectionFraud.FraudDetection.FraudDetectionMapper2;
import ElectionFraud.FraudDetection.FraudDetectionMapper3;
import ElectionFraud.FraudDetection.FraudDetectionReducer;
import ElectionFraud.FraudDetection.FraudDetectionReducer2;

public class CountVotersChangedParty {
	public static class CountVotersChangedPartyMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			IntWritable one = new IntWritable(1);

			word.set(value.toString().split("\t")[0] + " " + value.toString().split("\t")[2]+" 2006");
			context.write(word, one);
		}
	}

	public static class CountVotersChangedPartyMapper2 extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			IntWritable one = new IntWritable(1);

			word.set(value.toString().split("\t")[0] + " " + value.toString().split("\t")[2]+" 2008");
			context.write(word, one);
		}
	}

	public static class CountVotersChangedPartyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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

	public static class CountVotersChangedPartyMapper3 extends Mapper<Object, Text, Text, IntWritable> {

		@SuppressWarnings("unused")
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			IntWritable integer = new IntWritable(Integer.parseInt(key.toString().split(" ")[1]));

			word.set(key.toString().split(" ")[0]);
			context.write(word, integer);
		}
	}

	public static class CountVotersChangedPartyReducer2 extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			List<Integer> countValues = new ArrayList<Integer>();
			for(int i = 0; i < 2; i++) {
				countValues.add(Integer.parseInt(values.iterator().next().toString()));
			}
			if(countValues.get(1) != countValues.get(0)) {
				sum++;
				result.set(sum);
				key.set(key+" "+countValues.get(0)+" "+countValues.get(1));
				context.write(key, result);
			}
		}
	}

	public static class CountVotersChangedPartyMapper4 extends Mapper<Object, Text, Text, IntWritable> {

		@SuppressWarnings("unused")
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			IntWritable integer = new IntWritable(1);

			word.set(key.toString().split(" ")[1] + " " + key.toString().split(" ")[2]);
			context.write(word, integer);
		}
	}

	public static class CountVotersChangedPartyReducer3 extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			result.set(count);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Count Voter");
		job.setJarByClass(CountVotersChangedParty.class);
		//job.setMapperClass(CountVotersChangedPartyMapper.class);
		job.setReducerClass(CountVotersChangedPartyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CountVotersChangedPartyMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CountVotersChangedPartyMapper2.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		boolean jobSuccess = job.waitForCompletion(true);

		if(jobSuccess) {
			Job job1 = new Job(conf, "Count Voter 1");
			job1.setJarByClass(CountVotersChangedParty.class);
			job1.setMapperClass(CountVotersChangedPartyMapper3.class);
			job1.setReducerClass(CountVotersChangedPartyReducer2.class);
			job1.setInputFormatClass(KeyValueTextInputFormat.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job1, new Path(args[2]+"/part-r-00000"));
			FileOutputFormat.setOutputPath(job1, new Path("data/output1"));
			job1.waitForCompletion(true);
		}

		if(jobSuccess) {
			Job job2 = new Job(conf, "Party change");
			job2.setJarByClass(CountVotersChangedParty.class);
			job2.setMapperClass(CountVotersChangedPartyMapper4.class);
			job2.setReducerClass(CountVotersChangedPartyReducer3.class);
			job2.setInputFormatClass(KeyValueTextInputFormat.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job2, new Path("data/output1/part-r-00000"));
			FileOutputFormat.setOutputPath(job2, new Path("data/output2"));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}
}
