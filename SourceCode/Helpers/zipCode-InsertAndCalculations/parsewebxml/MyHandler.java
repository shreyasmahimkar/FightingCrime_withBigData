package parsewebxml;

import java.io.IOException;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import au.com.bytecode.opencsv.CSVParser;

public class MyHandler extends DefaultHandler {
	CSVParser csvp = new CSVParser(';');
	String zip;
	String psc;

	boolean ln = false;
	boolean tp = false;

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {

		if (qName.equalsIgnoreCase("postcode")) {
			// System.out.println("2");
			ln = true;
		}
	}

	@Override
	public void characters(char ch[], int start, int length)
			throws SAXException {

		if (ln) {

			String s = new String(ch, start, length);
			
			try {
				int z = Integer.parseInt(s);
			} catch (NumberFormatException ne) {
				try {
					String[] s1 = csvp.parseLine(s);
					s = s1[0];
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println(s);
			XMLParserSAX.zip = s;
			ln = false;
		}
	}
}