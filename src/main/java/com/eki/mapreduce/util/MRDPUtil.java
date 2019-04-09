package com.eki.mapreduce.util;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public class MRDPUtil {

    public static Map<String, String> transformXml2Map (String xml) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");

            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String value = tokens[i + 1];
                map.put(key.substring(0, key.length() - 1), value);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }
        return map;
    }

    public static Element getXmlElementFromString (DocumentBuilderFactory dbf, String xml) {
        try {
            DocumentBuilder builder = dbf.newDocumentBuilder();

            return builder.parse(new InputSource(new StringReader(xml))).getDocumentElement();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void copyAttributesToElement (NamedNodeMap attributes, Element element) {
        for (int i = 0; i < attributes.getLength(); ++i) {
            Attr toCopy = (Attr) attributes.item(i);
            element.setAttribute(toCopy.getName(), toCopy.getValue());
        }
    }

    public static String transformDocumentToString (Document document) {
        try {
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();

            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(document), new StreamResult(writer));

            return writer.getBuffer().toString().replaceAll("\n|\r", "");

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}
