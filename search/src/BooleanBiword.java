import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public class BooleanBiword {

	/**
	 * @param args
	 */
	static Map<String, ArrayList<String>> termIncidenceMatrix = new HashMap<String, ArrayList<String>>();

	public static void main(String[] args) {

		//String Query = args[0];
		String Query = "bill billion AND volger deputi";
		String[][] parsedQuery =  null;
		boolean oneWord = false;
		if(Query.split(" ").length>1)
		parsedQuery = parseQuery(Query);
		else 
		oneWord = false;	
			try {
				//File inputFile = new File(args[1]);
				File inputFile = new File("data\\output\\part-r-00000");
				BufferedReader br = new BufferedReader(
						new FileReader(inputFile));
				String line;
				String[] row = null;
				while ((line = br.readLine()) != null) {
					row = line.split(":");
					termIncidenceMatrix.put(row[0].substring(0,
							row[0].length() - 1).trim(), new ArrayList<String>(Arrays
							.asList(row[1].trim().split(","))));
				}
				br.close();
				ArrayList<String> firstMatches = new ArrayList<String>();
				ArrayList<String> secondMatches = new ArrayList<String>();
				ArrayList<String>  tempList = null;
				if(!oneWord){
				for (int j = 0; parsedQuery[j][0] != null; j++) {
					
					if(!parsedQuery[j][1].equals("prev")){
						tempList = termIncidenceMatrix.get(parsedQuery[j][1]);
						if(tempList != null)
						firstMatches.addAll(tempList);
						else{
							if(firstMatches.size()>0){
								System.out.println(firstMatches);
							}
							else{
								System.out.println("Not Found");
							}
						}
					}
					tempList = termIncidenceMatrix.get(parsedQuery[j][2]);
					if(tempList!=null){
						secondMatches.clear();
					secondMatches.addAll(tempList);
					}
					else{
						if(firstMatches.size()>0){
							System.out.println(firstMatches);
						}
						else{
							System.out.println("Not Found");
						}
					}
					if(parsedQuery[j][0].equals("AND")){
						firstMatches.retainAll(secondMatches);
					}
					if(parsedQuery[j][0].equals("NOT")){
						boolean found = false;
						String term = "";
						if(firstMatches.size() >= secondMatches.size()){
							ListIterator<String> li = secondMatches.listIterator();
							while(li.hasNext()){
								term = li.next();
								for(String st:firstMatches){
									if(st.trim().endsWith(term)){
										found = true;
										term = st;
										break;
									}
								}
								firstMatches.remove(term);
								found = false;
							}
						}
						else{
							 found = false;
							ListIterator<String> li = firstMatches.listIterator();
							while(li.hasNext()){
								term = li.next();
								for(String st:secondMatches){
									if(st.trim().endsWith(term)){
										found = true;
										term = st;
										break;
									}
								}
								secondMatches.remove(term);
								found = false;
							}
							firstMatches.clear();
							firstMatches.addAll(secondMatches);
						}
					}
					if(parsedQuery[j][0].equals("OR"))
					{
						firstMatches.addAll(secondMatches);
						Set<String>  temp  = new HashSet<String>();
						temp.addAll(firstMatches);
						String [] setArray = new String[temp.size()];
						temp.toArray(setArray);
						firstMatches.clear();
						firstMatches.addAll(new ArrayList<String>(Arrays.asList(setArray)));
						
					}					
				}
				System.out.println(firstMatches);
				}
				else{
					if(null!=termIncidenceMatrix.get(Query.trim())){
						System.out.println(termIncidenceMatrix.get(Query.trim()));
					}
					else{
						System.out.println("Not Found");
					}
				}
			} catch (IOException e) {
			}
	}

	static String[][] parseQuery(String query) {
		String[][] symbolTable = new String[20][5];

		StringTokenizer st = new StringTokenizer(query);
		String token = "";
		int index = 1, row = 0;
		boolean opCaptured = false;
		int tokCount = 0;
		while (st.hasMoreTokens()) {
			token = st.nextToken();

			if (token.equals("AND") && opCaptured == false) {
				symbolTable[row][0] = "AND";
				opCaptured = true;
			} else if (token.equals("AND") && opCaptured == true) {
				System.out.println("Bad Query Format");
				return null;
			}
			if (token.equals("OR") && opCaptured == false) {
				symbolTable[row][0] = "OR";
				opCaptured = true;

			} else if (token.equals("OR") && opCaptured == true) {
				System.out.println("Bad Query Format");
				return null;
			}
			if (token.equals("NOT")) {
				
				symbolTable[row][0] = "NOT";
				opCaptured = true;
			}
			else if (token.equals("NOT") && opCaptured == true) {
				System.out.println("Bad Query Format");
				return null;
			}
			if (opCaptured == false) {
				symbolTable[row][index] = token;
				tokCount++;
				index++;
			}

			if (tokCount == 4) {
				tokCount = 2;
				index = 1;
				row++;
				symbolTable[row][index] = "prev";
				index++;
				symbolTable[row][index] = "prev";
				index++;
			}
			opCaptured = false;
		}
		for(int i=0;i<symbolTable.length;i++){
			symbolTable[i][1]=symbolTable[i][1]+" "+ symbolTable[i][2];
			symbolTable[i][2]=symbolTable[i][3]+" "+symbolTable[i][4];
			symbolTable[i][3] = symbolTable[i][4] = null;
		}
		return symbolTable;
	}

}