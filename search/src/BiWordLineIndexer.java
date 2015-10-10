import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BiWordLineIndexer extends Configured implements Tool {

	public static class BiWordLineIndexMapper extends
			Mapper<Text, BytesWritable, Text, IntWritable> {

		private final static Text word = new Text();
		private final static IntWritable location = new IntWritable();

		@Override
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			ReutersDoc doc = StringUtils.getXMLContent(value);
			location.set(Integer.parseInt(doc.getDocID().toString()));
			String[] biWords = new String[2];
			boolean first = true;

			StringTokenizer itr = new StringTokenizer(
					StringUtils.normalizeText(doc.getContent()), "::");
			while (itr.hasMoreTokens()) {
				if (fillBiword(biWords, first, itr)) {
					word.set(biWords[0] + " " + biWords[1]);
					context.write(word, location);
					first = false;
				}
			}
		}
	}

	static boolean fillBiword(String[] biWord, boolean flag, StringTokenizer st) {
		String tmp = st.nextToken();
		boolean found = false;
		if ((tmp.trim().equals("") || tmp.trim().equals("a")
				|| tmp.trim().equals("aa") || tmp.trim().equals("aaa")
				|| tmp.trim().equals("aaaa") || tmp.trim().equals("aaaaaa") || tmp
				.trim().equals("aaaaaaaaa"))) {
			found = false;
		} else {
			if (flag == true) {
				biWord[0] = stemIt(tmp);
				if (st.hasMoreTokens())
					biWord[1] = stemIt(st.nextToken());
			} else {
				biWord[0] = biWord[1];
				biWord[1] = stemIt(tmp);
			}
			found = true;
		}
		return found;
	}

	private static String stemIt(String word) {
		char[] w = new char[501];
		Stemmer s = new Stemmer();
		char arr[] = word.toCharArray();
		String stemmedWord = null;
		for (int i = 0; i < arr.length;)

		{
			int ch = arr[i];
			i++;
			if (Character.isLetter((char) ch)) {
				int j = 0;
				while (true) {
					ch = Character.toLowerCase((char) ch);
					w[j] = (char) ch;
					if (j < 500)
						j++;
					if (i < arr.length) {
						ch = arr[i];
						i++;
					} else {

						/* to test add(char ch) */
						for (int c = 0; c < j; c++)
							s.add(w[c]);

						/* or, to test add(char[] w, int j) */
						/* s.add(w, j); */

						s.stem();
						{
							String u;

							
							u = s.toString();

							stemmedWord = u;
						}
						break;
					}
				}
			}
			if (ch < 0)
				break;
		}
		return stemmedWord;
	}

	public static class BiWordLineIndexReducer extends
			Reducer<Text, IntWritable, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException {

			Map<Integer, Boolean> docs = new TreeMap<Integer, Boolean>();
			Integer doc = null;
			boolean first = true;
			boolean added = false;
			for (IntWritable val : values) {
				if (!first && added)
					first = false;
				added = false;
				doc = val.get();
				docs.put(doc, false);

			}
			try {
				context.write(key, new Text(": "
						+ docs.keySet().toString().replaceAll("[\\]\\[]", "")));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private static final String OUTPUT_PATH = "data\\output";

	// where to read the data from.
	private static final String INPUT_PATH = "data\\txt1";

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "Word Frequence In Document");

		job.setMapperClass(BiWordLineIndexMapper.class);
		job.setReducerClass(BiWordLineIndexReducer.class);
		job.setJarByClass(BiWordLineIndexer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(ZipFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
/*
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));*/

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BiWordLineIndexer(),
				args);
		System.exit(res);
	}
}