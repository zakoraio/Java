
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
/**
 * WordFrequenceInDocument Creates the index of the words in documents,
 * mapping each of them to their frequency.
 */
public class WordFrequenceInDocument extends Configured implements Tool {
 
    // where to put the data in hdfs when we're done
    private static final String OUTPUT_PATH = "data\\output";
 
    // where to read the data from.
    private static final String INPUT_PATH = "data\\txt1";
    
  
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "Word Frequence In Document");
 
        
        job.setMapperClass(WordFrequenceInDocMapper.class);
        job.setReducerClass(WordFrequenceInDocReducer.class);
        job.setCombinerClass(WordFrequenceInDocReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setInputFormatClass(ZipFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
		/* FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));*/
        job.setJarByClass(WordFrequenceInDocument.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordFrequenceInDocument(), args);
        System.exit(res);
    }
}