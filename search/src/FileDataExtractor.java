import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class FileDataExtractor {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub InputStream stdout = new
		// StreamGobbler(sess.getStdout());

		FileReader resultFile = new FileReader(args[0]);

		BufferedReader br = new BufferedReader(resultFile);

		String DocId = "";
		String[] row = null;
		int counter = 0;
		while (true)

		{

			String line = br.readLine();

			if (line == null || counter > 20)

				break;
			else {
				counter++;
				row = line.split(" ");
				DocId = row[row.length - 1];
				System.out.println("~(" + DocId + ")~");

				try {
					FileReader file = new FileReader(args[1] + DocId
							+ "newsML.txt");

					BufferedReader br1 = new BufferedReader(file);

					while (true) {

						line = br1.readLine();
						if (line == null)

							break;
						else
							System.out.println(line);
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}
}
