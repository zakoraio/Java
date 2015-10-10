import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class UniWordIndexer extends Configured implements Tool{

	 public static class LineIndexMapper extends Mapper<Text, BytesWritable, Text, Text> {

		    private final static Text word = new Text();
		    private final static Text location = new Text();

		    public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
		   	 ReutersDoc doc = StringUtils.getXMLContent(value);
		        location.set(doc.getDocID());
		        String words[] = StringUtils.normalizeText(doc.getContent()).split("::");
		        
		        for (int i = 0; i < words.length; i++) {
		            StringBuilder buf = new StringBuilder();
		            if (!words[i].trim().equals("")&&!words[i].trim().equals("a") && !words[i].trim().equals("aa") 
		           		 && !words[i].trim().equals("aaa") && !words[i].trim().equals("aaaa") && !words[i].trim().equals("aaaaaa")
		           		 && !words[i].trim().equals("aaaaaaaaa")) {
		            	
		                word.set(stemIt(words[i]));
		                buf.append(doc.getDocID());
		                context.write(word, location);
		            }
		        }
		   }
		  }

	 
	 private static String stemIt(String word){
		 char[] w = new char[501];
	     Stemmer s = new Stemmer();
		 char arr[] = word.toCharArray();
		 String stemmedWord = null;
		 for(int i=0;i<arr.length;)

	    {  int ch = arr[i];i++;
	       if (Character.isLetter((char) ch))
	       {
	          int j = 0;
	          while(true)
	          {  ch = Character.toLowerCase((char) ch);
	             w[j] = (char) ch;
	             if (j < 500) j++;
	             if(i<arr.length){
	             ch = arr[i];i++;
	             }
	             else
	             {
	            	
	                /* to test add(char ch) */
	                for (int c = 0; c < j; c++) s.add(w[c]);

	                /* or, to test add(char[] w, int j) */
	                /* s.add(w, j); */

	                s.stem();
	                {  String u;

	                   /* and now, to test toString() : */
	                   u = s.toString();

	                   /* to test getResultBuffer(), getResultLength() : */
	                   /* u = new String(s.getResultBuffer(), 0, s.getResultLength()); */
	                   stemmedWord =  u;
	                }
	                break;
	             }
	          }
	       }
	       if (ch < 0) break;
	    }
		 return stemmedWord;
	}
	 
	 public static class LineIndexReducer extends Reducer<Text, Text, Text, Text> {
			@Override
		    protected void reduce(Text key, Iterable<Text> values, Context context)
		        throws IOException, InterruptedException {

		    	Set<String> docs = new HashSet<String>();
		    	String text = null;
		      boolean first = true;
		      boolean added = false;
		      StringBuilder toReturn = new StringBuilder();
		      for (Text val : values) {
		        if (!first && added)
		          toReturn.append(", ");
		        first=false;
		        added=false;
		        text = val.toString();
		        if(docs.add(text)){
		        toReturn.append(text);
		        added = true;
		        }
		      }
		    	  context.write(key, new Text(":" + toReturn.toString().substring(0, toReturn.toString().length()-2)));
		    }
		  }
  // where to put the data in hdfs when we're done
  private static final String OUTPUT_PATH = "data\\output";

  // where to read the data from.
  private static final String INPUT_PATH = "data\\txt1";
  


  public int run(String[] args) throws Exception {

      Configuration conf = getConf();
      Job job = new Job(conf, "Word Frequence In Document1");

      
      job.setMapperClass(LineIndexMapper.class);
      job.setReducerClass(LineIndexReducer.class);
     // job.setCombinerClass(LineIndexReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      
      job.setInputFormatClass(ZipFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

     /* FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
      FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));*/

		 FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setJarByClass(UniWordIndexer.class);
      return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new UniWordIndexer(), args);
      System.exit(res);
  }
}