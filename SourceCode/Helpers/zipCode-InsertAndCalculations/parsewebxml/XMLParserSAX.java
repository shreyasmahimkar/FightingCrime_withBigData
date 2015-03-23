package parsewebxml;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class XMLParserSAX {
	static String zip = null;

	public static String ParseXML(String xmlstring) {
		SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
		try {
			SAXParser saxParser = saxParserFactory.newSAXParser();
			MyHandler handler = new MyHandler();
			InputSource source  = new InputSource(new StringReader(xmlstring));
			saxParser.parse(source, handler);
			return zip;
		} catch (ParserConfigurationException | SAXException | IOException e) {
			e.printStackTrace();
			return zip;
		}
	}

}