/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.ext.processors;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.apache.commons.io.FilenameUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.annotation.CapabilityDescription;
import org.apache.nifi.processor.annotation.EventDriven;
import org.apache.nifi.processor.annotation.OnAdded;
import org.apache.nifi.processor.annotation.OnRemoved;
import org.apache.nifi.processor.annotation.OnScheduled;
import org.apache.nifi.processor.annotation.OnUnscheduled;
import org.apache.nifi.processor.annotation.SideEffectFree;
import org.apache.nifi.processor.annotation.SupportsBatching;
import org.apache.nifi.processor.annotation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.eclipse.birt.core.framework.Platform;
import org.eclipse.birt.report.engine.api.EngineConfig;
import org.eclipse.birt.report.engine.api.EngineConstants;
import org.eclipse.birt.report.engine.api.EngineException;
import org.eclipse.birt.report.engine.api.IEngineTask;
import org.eclipse.birt.report.engine.api.IRenderOption;
import org.eclipse.birt.report.engine.api.IReportEngine;
import org.eclipse.birt.report.engine.api.IReportEngineFactory;
import org.eclipse.birt.report.engine.api.IReportRunnable;
import org.eclipse.birt.report.engine.api.IRunAndRenderTask;
import org.eclipse.birt.report.engine.api.RenderOption;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"xml", "birt", "report"})
@CapabilityDescription("Creates PDF/DOC/EXCEL report using flowfile XML payload as data input. A new FlowFile is created "
        + "with report document content and is routed to the 'success' relationship. If the report generation "
        + "fails, the original FlowFile is routed to the 'failure' relationship")
public class BirtReport extends AbstractProcessor {

    public static final PropertyDescriptor DESIGN_FILE_NAME = new PropertyDescriptor.Builder()
            .name("Birt design file name")
            .description("Provides the name (including full path) of the birt design file to apply to the flowfile XML content.")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final AllowableValue OUTPUT_FORMAT_PDF = new AllowableValue("PDF", "PDF",
    		"PDF");
    
    public static final AllowableValue OUTPUT_FORMAT_DOC = new AllowableValue("DOC", "DOC",
    		"Microsoft Word format");
    
    public static final AllowableValue OUTPUT_FORMAT_XLS = new AllowableValue("XLS", "XLS",
    		"Microsoft Excel format");
    
    public static final AllowableValue OUTPUT_FORMAT_ODT = new AllowableValue("ODT", "ODT",
    		"Open Document Format (OpenOffice, LibreOffice)");
    
    public static final PropertyDescriptor OUTPUT_FORMAT = new PropertyDescriptor.Builder()
    .name("document format")
    .description("Generated document format")
    .allowableValues(OUTPUT_FORMAT_PDF, OUTPUT_FORMAT_DOC, OUTPUT_FORMAT_XLS,OUTPUT_FORMAT_ODT)
    .defaultValue(OUTPUT_FORMAT_PDF.getValue())
    .required(true)
    .build();    

    public static final PropertyDescriptor LOG_FILES_DIR = new PropertyDescriptor.Builder()
    .name("Birt log files directory")
    .description("Birt log files directory.")
    .defaultValue("/var/log/birt")
    .required(true)
    .addValidator(StandardValidators.createDirectoryExistsValidator(true, true))
    .build();

    
    
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
    		.description("The FlowFile with report content will be routed to this relationship").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
    		.description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid XML), it will be routed to this relationship").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    /** Birt report engine. BIRT report engine is thread-safe */
    private volatile IReportEngine engine;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DESIGN_FILE_NAME);
        properties.add(OUTPUT_FORMAT);
        properties.add(LOG_FILES_DIR);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @OnScheduled
    public void createEngine(final ProcessContext context) {
    	String logDir = context.getProperty(LOG_FILES_DIR).getValue();
    	engine = createEngine(logDir);
    }
    
    @OnUnscheduled
    public void destroyEngine(final ProcessContext context) {
    	engine.destroy();
    	Platform.shutdown();
    	engine = null;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }
        
        if (engine==null)
        	throw new ProcessException("Birt report engine is not created");

        final ProcessorLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);
        
        try {
            final String outFormat = context.getProperty(OUTPUT_FORMAT).getValue();
            FlowFile transformed = session.write(original, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream out) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn)) {

                        File designFile = new File(context.getProperty(DESIGN_FILE_NAME).getValue());
                        render(engine, rawIn, out, designFile, outFormat);
                        
                    } catch (final Exception e) {
                        throw new ProcessException(e);
                    }
                }
            });
            
            // create new filename with appropriate extension
            final String filename = original.getAttribute(CoreAttributes.FILENAME.key());
            String basename = FilenameUtils.getBaseName(filename);
            String newFilename = basename + "." + outFormat; 
            transformed = session.putAttribute(transformed, CoreAttributes.FILENAME.key(), newFilename);
            
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            logger.info("Transformed {}", new Object[]{original});
        } catch (Exception e) {
            logger.error("Unable to transform {} due to {}", new Object[]{original, e});
            session.transfer(original, REL_FAILURE);
        }
    }

	public static void render(final IReportEngine engine, final InputStream rawIn, final OutputStream out, File designFile, String outFormat) throws IOException,
			EngineException {

		InputStream designStream = new FileInputStream(designFile);
		try {
			IReportRunnable design = engine.openReportDesign(designStream);
			IRunAndRenderTask task = engine.createRunAndRenderTask(design);
			try {
	
				HashMap<String, Object> contextMap = new HashMap<String, Object>();
				contextMap.put("org.eclipse.datatools.enablement.oda.xml.inputStream", rawIn);
	
				// Set parent classloader for engine
				task.getAppContext().put(EngineConstants.APPCONTEXT_CLASSLOADER_KEY, BirtReport.class.getClassLoader());
	
				task.setErrorHandlingOption(IEngineTask.CANCEL_ON_ERROR);
				
				// possibly no effect
				// contextMap.put("org.eclipse.birt.report.data.oda.xml.closeInputStream", false);
	
				task.setAppContext(contextMap);
	
				// val outputStream = new ByteArrayOutputStream
	
				IRenderOption options = new RenderOption();
				// options.setOutputFileName("/tmp/test/result.pdf");
				options.setOutputStream(out);
				options.setOutputFormat(outFormat);
	
				task.setRenderOption(options);
				task.run();
				List<Throwable> errors = task.getErrors();
				evaluateStatus(task, "");
				
			} finally {
				task.close();
			}

		} finally {
			designStream.close();
		}

	}
    
	public static IReportEngine createEngine(String logDir) throws ProcessException {
		try {
			EngineConfig engineConfig = new EngineConfig();

			engineConfig.setLogConfig(logDir, Level.WARNING);
			Platform.startup(engineConfig);
			IReportEngineFactory factory = (IReportEngineFactory) Platform.createFactoryObject(IReportEngineFactory.EXTENSION_REPORT_ENGINE_FACTORY);
			IReportEngine engine = factory.createReportEngine(engineConfig);
			engine.changeLogLevel(Level.WARNING);
			return engine;
		} catch (Exception ex) {
			throw new ProcessException("Failed to create Birt report engine due to {}", ex);
		}
	}

	private static void evaluateStatus(IRunAndRenderTask task, String reportName) {
		int status = task.getStatus();
	    if (task.getStatus() == IEngineTask.STATUS_CANCELLED) {
	        String message = "report failed: " + reportName;
	        List<Throwable> errors = task.getErrors();
	        if (!errors.isEmpty()) {
	            throw new RuntimeException(message, errors.get(errors.size() - 1));
	        }
	        throw new RuntimeException(message);
	    }
	}	
	
}
