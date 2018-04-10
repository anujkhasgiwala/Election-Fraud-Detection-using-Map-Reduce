package palindrome;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PalindromeCounter {
	public static class PalindromeMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text word = new Text();
			IntWritable one = new IntWritable(1);

			StringTokenizer wordTokens = new StringTokenizer(value.toString());
			while(wordTokens.hasMoreTokens()) {
				String wordToken = wordTokens.nextToken();

				// Check if palindrome
				boolean isWordPalindrome = true;

				for(int i = 0; i < wordToken.length(); i++)
					if(wordToken.charAt(i) != wordToken.charAt(wordToken.length()-i-1)) {
						isWordPalindrome = false;
						break;
					}

				if(isWordPalindrome) {
					word.set(wordToken);
					context.write(word, one);
				}
			}
		}
	}   

	public static class PalindromeReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Counting Palindrome");
		job.setJarByClass(PalindromeCounter.class);
		job.setMapperClass(PalindromeMapper.class);
		job.setReducerClass(PalindromeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
