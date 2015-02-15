package org.apache.nifi.ext.processors;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.birt.report.engine.api.EngineException;
import org.eclipse.birt.report.engine.api.IReportEngine;
import org.junit.Ignore;
import org.junit.Test;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.jdom2.*;
import org.jdom2.xpath.XPath;

public class TestBirtReport {

	@Test
	public void testCreateEngine() {
		IReportEngine engine = BirtReport.createEngine("/var/log/birt");
		assertNotNull(engine);
	}

	/**
	 * 	Read input file customers.xml which contain many customers.
	 * Create separate xml for each customer and generate PDF for each customer.
	 * 
	 */	
//	@Test
	public void testRender() throws JDOMException, IOException, EngineException {
		
		Path xmlpath = Paths.get("src/test/resources/TestBirtReport/customers.xml");
		assertNotNull(xmlpath);
		
		Path designpath = Paths.get("src/test/resources/TestBirtReport/Customer.rptdesign");
		assertNotNull(designpath);

		SAXBuilder sax = new SAXBuilder(); 
		Document doc = sax.build( xmlpath.toFile() );
		Element root = doc.getRootElement();
		assertNotNull(root);
		
		List<Element> customerEelements = (List<Element>) XPath.selectNodes( root, "T");
		assertNotNull(customerEelements);
		assertTrue(customerEelements.size()>0);
		
		IReportEngine engine = BirtReport.createEngine("/var/log/birt");
		assertNotNull(engine);		
		
		System.out.println("nr of customers " + customerEelements.size());
        final StopWatch stopWatch = new StopWatch(true);
		
		// let's create separate XML documents for each customer
		// and create PDF for each customer
        
        int counter = 0;
        
		for (Element element : customerEelements) {
		    Element rootx = new Element("Customer");
		    Document docx  = new Document(rootx);
		    List<Content> content = element.cloneContent();
		    rootx.addContent(content);
		    
		    ByteArrayOutputStream xmlstream = new ByteArrayOutputStream();
		    XMLOutputter outputter = new XMLOutputter(Format.getPrettyFormat());
		    outputter.output(docx,xmlstream);
		    xmlstream.close();		    
//		    System.out.println(xmlstream);

		    ByteArrayInputStream rawIn = new ByteArrayInputStream(xmlstream.toByteArray());
		    
		    ByteArrayOutputStream out = new ByteArrayOutputStream();
		    
		    String outFormat = "PDF";
		    BirtReport.render(engine, rawIn, out, designpath.toFile(), outFormat);
		    
		    counter++;
		    
	/***	Just to verify results are indeed pdf documents     
		    FileOutputStream pdffile = new FileOutputStream("/tmp/cust" + counter + ".pdf");
		    pdffile.write(out.toByteArray());
		    pdffile.close();
	***/	    
		}
		
		System.out.println("total time " + stopWatch.getElapsed(TimeUnit.MILLISECONDS) + " msecs");
	}

	/**
	 *	This test wont work.
	 *  Birt engine won't throw SAXParseException and reports success.
	 *  SAXParseException only goes to log. 
	 * 
	 */
	@Ignore
    @Test
    public void testNonXmlContent() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new BirtReport());
        runner.setProperty(BirtReport.DESIGN_FILE_NAME, "src/test/resources/TestBirtReport/Customer.rptdesign");
        runner.setProperty(BirtReport.LOG_FILES_DIR, 	"/var/log/birt");
        runner.setProperty(BirtReport.OUTPUT_FORMAT, 	"PDF");

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue("not xml".getBytes(), attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(BirtReport.REL_FAILURE);
        final MockFlowFile original = runner.getFlowFilesForRelationship(BirtReport.REL_FAILURE).get(0);
        final String originalContent = new String(original.toByteArray(), StandardCharsets.UTF_8);
        System.out.println("originalContent:\n" + originalContent);

        original.assertContentEquals("not xml");
    }

	/**
	 * 
	 */
    @Test
    public void testXmlContent() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new BirtReport());
        runner.setProperty(BirtReport.DESIGN_FILE_NAME, "src/test/resources/TestBirtReport/Customer.rptdesign");
        runner.setProperty(BirtReport.LOG_FILES_DIR, 	"/var/log/birt");
        runner.setProperty(BirtReport.OUTPUT_FORMAT, 	"PDF");
        
		Path xmlpath = Paths.get("src/test/resources/TestBirtReport/CustSample.xml");
		assertNotNull(xmlpath);
		byte [] fileData = Files.readAllBytes(xmlpath);
        
        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue(fileData, attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(BirtReport.REL_SUCCESS);
        final MockFlowFile original = runner.getFlowFilesForRelationship(BirtReport.REL_SUCCESS).get(0);
        final String originalContent = new String(original.toByteArray(), StandardCharsets.UTF_8);
   //     System.out.println("originalContent:\n" + originalContent);

        assertTrue( originalContent.contains("PDF") );
    }

}
