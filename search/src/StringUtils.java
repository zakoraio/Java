



import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.hadoop.io.BytesWritable;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 *
 * @author hadoop
 */
public class StringUtils {

    private static final String[] punctuation = {"`", "&", "'", "*", "\\", "{", "}", "[",
        "]", ":", ",", "!", ">", "<", "#", "(", ")", "%", ".",
        "+", "?", "\"", ";", "/", "^", "", "|", "~", "+","$","=","@","_","-"};
    private static final String[] stop_words = {"a", "an", "and", "are", "as", "at", "be", "by",
        "for", "from", "has", "he", "in", "is", "it", "its", "of", "on", "that", "the","aa","aaa","aaaa","aaaaaa","aaaaaaaaa","aad",
        "to", "was", "were", "will", "with","how","about","that","this","when","who","com","where","www","may","might","his","her","him",
        "because","been","before","can't","cannot","doesn't","doing","don't","down","each","few","had","hadn't","once","no","nor","not",
        "more","most","mustn't","out","our","ours","between","both","but","didn't","do","all","against","again","i'd","i'll","lets","let","me",
        "ourselves","she","he","should","shouldn't","some","such","their","them","themselves","those","too","through","which","while"};

    public static String normalizeText(String str) {
    	
        //replace punctuation
    	
        for (String punc : punctuation) {
            str = str.replace(punc, "");
        }
        str = str.replaceAll("\\d","");
        str = str.toLowerCase();
        str = " "+str+" ";
        //replace stop words
        for (String stop_word : stop_words) {
            str = str.replace(" " + stop_word + " ", " ");
        }

        str = str.replace("-", " ").replace(" ", "::")
                .replace("\n", "::").replace("\t", "::")
                .replace("\r", "::");
        return str.trim();
    }

    public static ReutersDoc getXMLContent(BytesWritable xmlStr) {
        //get the xml factory and parse the xml file
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document dom;
        StringBuilder docContent = new StringBuilder();
        ReutersDoc doc = new ReutersDoc();
        try {
            //Using factory get an instance of document builder
            DocumentBuilder db = dbf.newDocumentBuilder();

            //parse using builder to get DOM representation of the XML file
            dom = db.parse(new ByteArrayInputStream(xmlStr.getBytes()));

            //now parse document
            //get document id
            Element root = dom.getDocumentElement();
            doc.setDocID(root.getAttribute("itemid"));

            //get title and headline and body of xml data
            docContent.append(root.getElementsByTagName("title").item(0).getTextContent().trim()).append(" ");
            docContent.append(root.getElementsByTagName("headline").item(0).getTextContent().trim()).append(" ");
            docContent.append(root.getElementsByTagName("text").item(0).getTextContent().trim());

            doc.setContent(docContent.toString());
        } catch (Exception pce) {
            pce.printStackTrace();
        }

        return doc;
    }
    
    @SuppressWarnings("deprecation")
	public static ReutersDoc getContent(BytesWritable xmlStr) throws UnsupportedEncodingException {
        //get the xml factory and parse the xml file
        ReutersDoc doc = new ReutersDoc();
            doc.setDocID("temp");
            doc.setContent(new String(xmlStr.get(),"UTF8"));
        return doc;
    }
    
    public static boolean isNumeric(String input) {
    	  try {
    	    Integer.parseInt(input);
    	    return true;
    	  }
    	  catch (NumberFormatException e) {
    	    // s is not numeric
    	    return false;
    	  }
    	}
}
