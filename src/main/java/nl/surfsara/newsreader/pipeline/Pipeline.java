/**
 * Copyright 2014 SURFsara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.surfsara.newsreader.pipeline;

import java.util.Properties;

import nl.surfsara.newsreader.pipeline.cascading.flows.NewsReaderFlow;
import nl.surfsara.newsreader.pipeline.modules.ModuleConstants;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.property.AppProps;

/**
 * Runnable class that runs the newsreader NLP pipeline on Hadoop clusters.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class Pipeline implements Runnable {
	private static final Logger logger = Logger.getLogger(Pipeline.class);
	private String[] args;

	public Pipeline(String[] args) {
		this.args = args;
	}

	@Override
	public void run() {
		PropertyConfigurator.configure("log4jconfig.properties");
		boolean showusage = false;
		if (args.length < 4) {
			showusage = true;
		} else {
			String inputPath = args[0];
			String outputPath = args[1];
			String errorPath = args[2];
			String layoutFile = args[3];
			String componentsCache = args[4];

			try {
				// Read the pipelinelayout 
				PipelineLayout pl = new PipelineLayout(layoutFile);
				logger.info("Running pipeline id: " + pl.getPipelineid());
				logger.info("Running pipeline version: " + pl.getPipelineversion());

				// Run the  pipeline
				Properties properties = new Properties();
				properties.setProperty("mapreduce.job.complete.cancel.delegation.tokens", "false");

				properties.put("mapreduce.task.timeout", "7200000");
				properties.put("mapreduce.job.cache.archives", componentsCache + "#" + ModuleConstants.ARCHIVEROOT);

				// Child jvm settings
				properties.put("mapreduce.map.java.opts", "-Xmx8G -Dfile.encoding=UTF-8");
				//properties.put("mapreduce.reduce.java.opts","");

				// Memory limits
				properties.put("mapreduce.map.memory.mb", "10240");
				//properties.put("mapreduce.reduce.memory.mb","");

				// Slow start reducers:
				properties.put("mapreduce.job.reduce.slowstart.completedmaps", "0.9");

				// Number of reducers
				properties.put("mapreduce.job.reduces", "5");

				AppProps.setApplicationJarClass(properties, Pipeline.class);
				HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

				NewsReaderFlow nrFlow = new NewsReaderFlow(pl);

				FlowDef flowDef = nrFlow.getFlowDefinition(inputPath, outputPath, errorPath);

				@SuppressWarnings("rawtypes")
				Flow flow = flowConnector.connect(flowDef);
				flow.writeDOT("newsreader.dot");
				flow.complete();
			} catch (Exception e) {
				logger.error(e);
				e.printStackTrace();
			}
		}
		if (showusage) {
			showUsage();
		}
	}

	private void showUsage() {
		System.out.println("Usage: ");
		System.out.println();
		System.out.println("The pipeline program runs the Newsreader pipeline on Hadoop.");
		System.out.println();
		System.out.println("The pipeline expects the following arguments: ");
		System.out.println(" 1.) an inputpath: a path on HDFS containing documents in sequencefile format (see the load tool). Wildcards allowed.");
		System.out.println(" 2.) an outputpath: a path on HDFS where output documents should be written.");
		System.out.println(" 3.) an errorpath: a path on HDFS where documents who failed to be processed should be stored.");
		System.out.println(" 4.) a components file: a path on HDFS to the components zip file.");
		System.out.println();
		System.out.println("A note on the components zip file: the newsreader components should be zipped and uploaded to Hadoop. Then, distributed cache is used");
		System.out.println("to distribute and symlink the components to all the compute nodes.");
		System.out.println();
		System.out.println("An example:");
		System.out.println("Run the pipeline on documents in /foo/*: yarn jar newsreader-hadoop.jar pipeline /foo/in/* /bar/out /bar/error /foo/components.zip");
		System.out.println();
	}
}
