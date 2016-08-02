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
package nl.surfsara.hadoop.mtchadoop.pipeline.flows.udfs;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import nl.surfsara.hadoop.mtchadoop.pipeline.modules.Module;
import nl.surfsara.hadoop.mtchadoop.pipeline.modules.ModuleExecutorService;
import nl.surfsara.hadoop.mtchadoop.pipeline.modules.PipelineStep;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/**
 * Function that executes the modules run script in a separate Thread.
 *
 * @author mathijs.kattenberg@surfsara.nl
 */
@SuppressWarnings("serial")
public class RunModuleFunction extends BaseOperation<Tuple> implements Function<Tuple> {
    private static final Logger logger = Logger.getLogger(RunModuleFunction.class);
    private ModuleExecutorService mes;
    private PipelineStep pipelineStep;
    private String localDir;

    // Eats: <docName, docContent, docFailed>
    // Emits: <docName, docContent, docFailed>
    public RunModuleFunction(PipelineStep pipelineStep) {
        super(3, new Fields("docName", "docContent", "docFailed"));
        this.pipelineStep = pipelineStep;
    }

    public RunModuleFunction(PipelineStep pipelineStep, Fields fields) {
        super(3, fields);
        this.pipelineStep = pipelineStep;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Tuple> call) {
        String[] taskId = flowProcess.getStringProperty("mapred.task.id").split("_");
        localDir = flowProcess.getStringProperty("job.local.dir");
        localDir = localDir + "/mo-" + taskId[3] + "-" + taskId[4].substring(1) + "/" + UUID.randomUUID().toString();
        File f = new File(localDir);
        f.mkdirs();
        mes = new ModuleExecutorService();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall<Tuple> operationCall) {
        super.cleanup(flowProcess, operationCall);
        mes.destroy();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        logger.error("Charset: " + Charset.defaultCharset());
        TupleEntry args = functionCall.getArguments();
        String docName = args.getString("docName");
        logger.info("Processing: " + docName);
        flowProcess.setStatus("Processing: " + docName);
        Tuple result = operate(args);
        functionCall.getOutputCollector().add(result);
    }

    protected Tuple operate(TupleEntry args) {
        Tuple result = new Tuple();
        String docName = args.getString("docName");
        byte[] docContent = ((BytesWritable) args.getObject("docContent")).getBytes();
        boolean docFailed = args.getBoolean("docFailed");
        if (docFailed) {
            logger.info("Skipping pipelineStep: " + pipelineStep.getName() + " for document: " + docName + " because of previous failure...");
            result.add(new Text(docName));
            result.add(new BytesWritable(docContent));
            result.add(true);
        } else {
            try {
                Module instance = pipelineStep.getInstance();
                instance.setDocumentKey(docName);
                instance.setInputDocument(docContent);
                instance.setLocalDirectory(localDir);
                long tstart = System.currentTimeMillis();
                FutureTask<Module> executeModule = mes.executeModule(instance);
                Module outputInstance = executeModule.get(pipelineStep.getTimeout(), TimeUnit.MILLISECONDS);
                byte[] outputDocument = outputInstance.getOutputDocument();
                boolean outputDocFailed = outputInstance.hasFailed();
                long tend = System.currentTimeMillis();
                logger.info("Applying pipelineStep: " + pipelineStep.getName() + " on document: " + docName + " took " + (tend - tstart) + " ms.");
                logger.info("Module " + pipelineStep.getName() + " result: " + !outputDocFailed + " on document: " + docName);
                result.add(new Text(docName));
                result.add(new BytesWritable(outputDocument));
                result.add(outputDocFailed);
            } catch (Exception e) {
                result.add(new Text(docName));
                result.add(new BytesWritable(docContent));
                result.add(true);
                logger.error(e);
            }
        }
        return result;
    }
}
