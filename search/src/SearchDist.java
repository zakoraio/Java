import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

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

public class SearchDist extends Configured implements Tool {
	private static String Query = "";
	byte a = 12;

	public static class SearchDistMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text val, Context context)
				throws IOException, InterruptedException {
			if (Query.equals("")) {
				Configuration conf = context.getConfiguration();
				Query = new String(conf.get("query"));
			}
			Query = Query.toLowerCase();
			StringTokenizer tokens = new StringTokenizer(Query);
			String[] wordAndDoc = val.toString().split("@");
			boolean matched = false;
			String token= null;
			while(tokens.hasMoreTokens()){
				token = tokens.nextToken();
				if(token.equals(wordAndDoc[0])){
					val=new Text(token+"@"+wordAndDoc[1]);
					matched = true;
				}
			}
			
			if (matched) {
				context.write(new Text(Query),val);
			}
		}
	}

	public static class SearchDistReducer extends
			Reducer<Text, Text, Text, Text> {


		static Map<String, Integer> wordsInQuery = new HashMap<String, Integer>();
		static Map<String, Double> termFrequencyOfQuery = new HashMap<String, Double>();
		static Map<String, HashMap<String, Tfidfdata>> idfOFDocument = new HashMap<String, HashMap<String, Tfidfdata>>();
		static String[] gileList = null;
		private static String Query = "";
	    static List<DataForRank> rList = new ArrayList<DataForRank>();
		
		
		public static double getTfFromText(String text) {
			String[] data = null;
			double num, deno;
			data = text.trim().split("/");
			num = Double.parseDouble(data[0]);
			deno = Double.parseDouble(data[1]);
			return num / deno;

		}

		public static double cosineSimilarity(double[] docVector1,
				double[] docVector2) {
			double dotProduct = 0.0;
			double magnitude1 = 0.0;
			double magnitude2 = 0.0;
			double cosineSimilarity = 0.0;

			for (int i = 0; i < docVector1.length; i++) {
				dotProduct += docVector1[i] * docVector2[i];
				magnitude1 += Math.pow(docVector1[i], 2);
				magnitude2 += Math.pow(docVector2[i], 2);
			}

			magnitude1 = Math.sqrt(magnitude1);
			magnitude2 = Math.sqrt(magnitude2);

			if (magnitude1 != 0.0 | magnitude2 != 0.0) {
				cosineSimilarity = dotProduct / (magnitude1 * magnitude2);
			} else {
				return 0.0;
			}
			return cosineSimilarity;
		}


		public static void getCosineSimilarity(Context context) throws IOException, InterruptedException {
			double[] vec1 = new double[idfOFDocument.size()];
			double[] vec2 = new double[idfOFDocument.size()];

			String name = "";
			
			StringTokenizer tokens1 = new StringTokenizer(Query);
			String token1 = "";
			Set<String> files = null;
			int index = 0 ;
			
			while (tokens1.hasMoreTokens()) {
				token1 = tokens1.nextToken();
				if(idfOFDocument.get(token1)!=null){
					files = idfOFDocument.get(token1).keySet();
					for(String file:files){
						gileList[index]=file;
						index++;
					}
				}
			}
			
			for (int i = 0; i < index; i++) {
				name = gileList[i];
				StringTokenizer tokens = new StringTokenizer(Query);
				String token = "";
				int j = 0;
				while (tokens.hasMoreTokens()) {

					token = tokens.nextToken();
					if (idfOFDocument.get(token).get(name) != null) {
						vec1[j] = idfOFDocument.get(token).get(name)
								.gettfIdf();
						vec2[j] = idfOFDocument.get(token).get(name)
								.queryTermIdf(termFrequencyOfQuery.get(token));
						
					}
					else{
						vec1[j] = 0.0;
						Set<String> tmpTfIdf = idfOFDocument.get(token).keySet();
						String docName = "";
						for (String tmpName : tmpTfIdf) {
							docName = tmpName;
							break;
						}
						vec2[j] = idfOFDocument.get(token).get(docName)
								.queryTermIdf(termFrequencyOfQuery.get(token));
					}
					j++;
				}
				
			//	context.write(new Text(name), new Text(new Double(cosineSimilarity(vec1,vec2)).toString()));
					rList.add(new DataForRank(name, cosineSimilarity(vec1,
							vec2)));

			}

		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokens;
			if (Query.equals("")) {
				Configuration conf = context.getConfiguration();
				Query = new String(conf.get("query"));
			}
			/*context.write(new Text(Query+key),new Text("Hello"));*/
			if(gileList==null){
				gileList = new String[200000];
			}
			String token="";
			String line = "";
			String[] row = null;
			String[] data = null;
			for(Text val:values){
				line = val.toString();
				row = line.split("@");
				 tokens = new StringTokenizer(Query);
				while (tokens.hasMoreTokens()) {
					token = tokens.nextToken();
					if (row[0].equals(token)) {
						data = row[1].split(",");
						Tfidfdata docsTfIdf = new Tfidfdata(
								row[1].split(" ")[0].split("\\[")[0].trim(),
								getTfFromText(data[1]), Double
										.parseDouble(data[2].replace(']', ' ')
												.trim()));
						if (idfOFDocument.get(row[0]) != null) {
							HashMap<String, Tfidfdata> tfidfMap = idfOFDocument
									.get(row[0]);
							tfidfMap.put(docsTfIdf.getDoc(), docsTfIdf);
						} else {
							HashMap<String, Tfidfdata> tfidfMap = new HashMap<String, Tfidfdata>();
							tfidfMap.put(docsTfIdf.getDoc(), docsTfIdf);
							idfOFDocument.put(row[0], tfidfMap);
						}
					}
				}
			}
			/*context.write(new Text(Query+key),new Text(((new Integer(idfOFDocument.size())).toString())
			+ (new Integer(Query.split(" ").length)).toString()));*/
		
			int val = 0;
			 tokens = new StringTokenizer(Query);
			while (tokens.hasMoreTokens()) {
				token = tokens.nextToken().toLowerCase();
				if (wordsInQuery.get(token) != null) {
					val = wordsInQuery.get(token);
					wordsInQuery.put(token, ++val);
				} else {
					wordsInQuery.put(token, 1);
				}
			}
			if (Query.split(" ").length != 1) {
				for (String word : wordsInQuery.keySet()) {
					termFrequencyOfQuery.put(word,
							((double) Query.split(" ").length / (double) wordsInQuery
									.get(word)));
				}
			} else {
				termFrequencyOfQuery.put(Query.trim(), 1.0);
			}
			
			getCosineSimilarity(context);
			Collections.sort(rList);
			for(DataForRank doc:rList){
			context.write(new Text(doc.toString().split("::::::::::::")[0]), new Text(doc.toString().split("::::::::::::")[1]));
			}
		}
	}
	// where to put the data in hdfs when we're done
	private static final String OUTPUT_PATH = "data\\output2";

	// where to read the data from.
	private static final String INPUT_PATH = "data\\inp2";

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		conf.set("query", args[2]);
		Job job = new Job(conf, "Final Phase");

		job.setMapperClass(SearchDistMapper.class);
		job.setReducerClass(SearchDistReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		/*FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));*/

		
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		job.setJarByClass(SearchDist.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String [] params = new String[3];
		int i =0;
		for(i=0;i<args.length;i++){
			params[i]=args[i];
		}
		FileReader file = new FileReader("extract/Query.txt");

		
		BufferedReader br1 = new BufferedReader(file);
		
		params[i] = br1.readLine();
		
		int res = ToolRunner.run(new Configuration(), new SearchDist(), params);
	    List<DataForRank> rList = new ArrayList<DataForRank>();
		
		System.exit(res);
	}

}
class Tfidfdata {
	String doc;
	double tf;
	double tfidf;
	double idf;
	double queryTermIdf;

	Tfidfdata(String doc, double tf, double tfidf) {
		this.doc = doc;
		this.tf = tf;
		this.tfidf = tfidf;
		this.idf = tfidf / tf;
	}

	public String getDoc() {
		return this.doc;
	}

	public double getTf() {
		return this.tf;
	}

	public double gettfIdf() {
		return this.tfidf;
	}

	public double getIdf() {
		return this.idf;
	}

	public double queryTermIdf(double queryTf) {
		return queryTf * idf;
	}
}

class DataForRank implements Comparable<DataForRank> {
	String docName;
	double tfidf;
	DataForRank(String doc, double tfidf){
		this.docName = doc;
		this.tfidf = tfidf;
	}
	public int compareTo(DataForRank rd) {
		int ret = 0;
		if (rd instanceof DataForRank) {
			if (this.tfidf == ((DataForRank) rd).tfidf) {
				ret = 0;
			} else
				ret = this.tfidf < ((DataForRank) rd).tfidf ? 1 : -1;
		}
		return ret;
	}
	public String toString(){
		return "Similarity = "+tfidf + "::::::::::::Doc= "+docName;
	}

}


