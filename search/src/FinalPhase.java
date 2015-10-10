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





public class FinalPhase extends Configured implements Tool{
	
	
	public static class FinalPhaseMapper extends
	Mapper<LongWritable, Text, Text, Text> {


public void map(LongWritable key, Text val, Context context)
		throws IOException, InterruptedException {
	String[] wordAndCounters = val.toString().split("\t");
	String[] wordAndDoc = wordAndCounters[0].split("@"); 
	context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1]
			+ "=" + wordAndCounters[1]));
	}
}
	public static class FinalPhaseReducer extends Reducer<Text, Text, Text, Text> { 

		public FinalPhaseReducer() {
		}
		private static int noOfDocsInCps;
		
		
		@Override
	    protected void reduce(Text key, Iterable<Text> values, Context context)
	        throws IOException, InterruptedException {
			
			if(noOfDocsInCps == 0){
			Configuration conf = context.getConfiguration();
			noOfDocsInCps = Integer.parseInt(conf.get("noOfDocs"));
			}
			int numberOfDocumentsInCorpusWhereKeyAppears = 0;
			Map<String, String> tempFrequencies = new HashMap<String, String>();
			String text = "";
			for(Text val:values){
				text = val.toString();
				String[] documentAndFrequencies = text.split("=");
				numberOfDocumentsInCorpusWhereKeyAppears++;
				tempFrequencies.put(documentAndFrequencies[0],
						documentAndFrequencies[1]);
			}
			for (String document : tempFrequencies.keySet()) {
				String[] wordFrequenceAndTotalWords = tempFrequencies.get(
						document).split("/");

				double tf = Double.valueOf(Double
						.valueOf(wordFrequenceAndTotalWords[0])
						/ Double.valueOf(wordFrequenceAndTotalWords[1]));

				double idf = (double) noOfDocsInCps
						/ (double) numberOfDocumentsInCorpusWhereKeyAppears;

				double tfIdf = noOfDocsInCps == numberOfDocumentsInCorpusWhereKeyAppears ? tf
						: tf * Math.log10(idf);

				context.write(new Text(key + "@" + document), new Text("'"
						+ numberOfDocumentsInCorpusWhereKeyAppears + "/"
						+ noOfDocsInCps + " , "
						+ wordFrequenceAndTotalWords[0] + "/"
						+ wordFrequenceAndTotalWords[1] + " , "
						+ (new Double(tfIdf)).toString() + "'"));
			}
		}
	}

	
	 // where to put the data in hdfs when we're done
	  private static final String OUTPUT_PATH = "data\\output2";

	  // where to read the data from.
	  private static final String INPUT_PATH = "data\\inp1";
	  


	  public int run(String[] args) throws Exception {

	      Configuration conf = getConf();
	      conf.set("noOfDocs", "16000");
	      Job job = new Job(conf, "Final Phase");

	      
	      job.setMapperClass(FinalPhaseMapper.class);
	      job.setReducerClass(FinalPhaseReducer.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      
	      job.setInputFormatClass(TextInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);
		  
      FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
	      FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

/*			 FileInputFormat.addInputPath(job, new Path(args[0]));
		        FileOutputFormat.setOutputPath(job, new Path(args[1]));
*/	      job.setJarByClass(FinalPhase.class);
	      return job.waitForCompletion(true) ? 0 : 1;
	  }

	  public static void main(String[] args) throws Exception {
	      int res = ToolRunner.run(new Configuration(), new FinalPhase(), args);
	      System.exit(res);
	  }
	
}
