package com.eki.mapreduce;

import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

public class xmlTest {
    @Test
    public void testCreateXmlString () {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder( );
            Document doc = db.newDocument();

            Element

        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
    }
}
