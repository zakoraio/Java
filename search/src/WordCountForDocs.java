import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCountForDocs extends Configured implements Tool {

	public static class WordCountForDocsMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		public WordCountForDocsMapper() {
		}

		public void map(LongWritable key, Text val, Context context)
				throws IOException, InterruptedException {
			String[] wordAndDocCounter = val.toString().split("\t");
			String[] wordAndDoc = wordAndDocCounter[0].split("@");
			context.write(new Text(wordAndDoc[1]), new Text(wordAndDoc[0]
					+ "=" + wordAndDocCounter[1]));
		}
	}

	public static class WordCountForDocsReducer extends Reducer<Text, Text, Text, Text> {

		@Override
	    protected void reduce(Text key, Iterable<Text> values, Context context)
	        throws IOException, InterruptedException {
			int sumOfWordsInDocument = 0;
			Map<String, Integer> tempCounter = new HashMap<String, Integer>();
			String text = "";
			for(Text val:values){
				text = val.toString();
				String[] wordCounter = text.split("=");
				tempCounter
						.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
				sumOfWordsInDocument += Integer.parseInt(text.split("=")[1]);
			}
			for (String wordKey : tempCounter.keySet()) {
				context.write(new Text(wordKey + "@" + key.toString()),
						new Text(tempCounter.get(wordKey) + "/"
								+ sumOfWordsInDocument));
			}
		}
	}

	 // where to put the data in hdfs when we're done
	  private static final String OUTPUT_PATH = "data\\output1";

	  // where to read the data from.
	  private static final String INPUT_PATH = "data\\inp";
	  


	  public int run(String[] args) throws Exception {

	      Configuration conf = getConf();
	      conf.set("mapred.child.java.opts", "-Xmx2000m");
	      Job job = new Job(conf, "Word Frequence In Document1");

	      
	      job.setMapperClass(WordCountForDocsMapper.class);
	      job.setReducerClass(WordCountForDocsReducer.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      
	      job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
	      FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

			/* FileInputFormat.addInputPath(job, new Path(args[0]));
		        FileOutputFormat.setOutputPath(job, new Path(args[1]));*/
	      job.setJarByClass(WordCountForDocs.class);
	      return job.waitForCompletion(true) ? 0 : 1;
	  }

	  public static void main(String[] args) throws Exception {
	      int res = ToolRunner.run(new Configuration(), new WordCountForDocs(), args);
	      System.exit(res);
	  }
}
