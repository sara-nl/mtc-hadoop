/**
 * Copyright 2016 SURFsara
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.surfsara.hadoop.mtchadoop.pipeline;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.property.AppProps;
import nl.surfsara.hadoop.mtchadoop.pipeline.flows.PipelineFlow;
import nl.surfsara.hadoop.mtchadoop.pipeline.modules.ModuleConstants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.Properties;

/**
 * Runnable class that runs a pipeline (series of functions) on Hadoop clusters.
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
        PropertyConfigurator.configure("log4j.properties");
        boolean showusage = false;
        System.out.println(args.length);
        if (args.length < 5) {
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

                // TODO Supply these at run time or via config
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

                PipelineFlow plFlow = new PipelineFlow(pl);

                FlowDef flowDef = plFlow.getFlowDefinition(inputPath, outputPath, errorPath);

                @SuppressWarnings("rawtypes")
                Flow flow = flowConnector.connect(flowDef);
                flow.writeDOT("pipeline.dot");
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
        System.out.println("The pipeline program runs a pipeline on Hadoop.");
        System.out.println();
        System.out.println("The pipeline expects the following arguments: ");
        System.out.println(" 1.) an inputpath: a path on HDFS containing input files in sequencefile format (see the load tool). Wildcards allowed.");
        System.out.println(" 2.) an outputpath: a path on HDFS where output files should be written.");
        System.out.println(" 3.) an errorpath: a path on HDFS where files who failed to be processed should be stored.");
        System.out.println(" 4.) a pipeline layout: path to a layout file describing the pipeline to run.");
        System.out.println(" 5.) a components file: a path on HDFS to the components zip file.");
        System.out.println();
        System.out.println("A note on the components zip file: the components for the pipeline should be zipped and uploaded to Hadoop. Then, distributed cache is used");
        System.out.println("to distribute and symlink the components to all the compute nodes.");
        System.out.println();
    }
}
