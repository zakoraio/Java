
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
 
/**
 * WordFrequenceInDocMapper implements the Job 1 specification for the TF-IDF algorithm
 */
public class WordFrequenceInDocMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
 
    public WordFrequenceInDocMapper() {
    }
 
    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();
   
    /**
     * @param key is the byte offset of the current line in the file;
     * @param value is the line from the file
     * @param output has the method "collect()" to output the key,value pair
     * @param reporter allows us to retrieve some information about the job (like the current filename)
     *
     *     POST-CONDITION: Output <"word", "filename@offset"> pairs
     */
    public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
    	 ReutersDoc doc = StringUtils.getXMLContent(value);
    	// System.out.println("Filename="+doc.getDocID());
      
         String words[] = StringUtils.normalizeText(doc.getContent()).split("::");
         for (int i = 0; i < words.length; i++) {
             StringBuilder buf = new StringBuilder();
             if (!words[i].trim().equals("")&&!words[i].trim().equals("a") && !words[i].trim().equals("aa") 
            		 && !words[i].trim().equals("aaa") && !words[i].trim().equals("aaaa") && !words[i].trim().equals("aaaaaa")
            		 && !words[i].trim().equals("aaaaaaaaa")) {
                 word.set(stemIt(words[i])+"@"+doc.getDocID());
                 buf.append(doc.getDocID()).append("::").append(i + 1);
                 context.write(word, one);
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
	 
}

