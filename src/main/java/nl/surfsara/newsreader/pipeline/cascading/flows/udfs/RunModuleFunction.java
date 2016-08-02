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
package nl.surfsara.newsreader.pipeline.cascading.flows.udfs;

import java.io.File;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import nl.surfsara.newsreader.pipeline.modules.Module;
import nl.surfsara.newsreader.pipeline.modules.ModuleExecutorService;
import nl.surfsara.newsreader.pipeline.modules.PipelineStep;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

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
		flowProcess.setStatus("Processing: " + docName);
		Tuple result = operate(args);
		functionCall.getOutputCollector().add(result);
	}

	protected Tuple operate(TupleEntry args) {
		Tuple result = new Tuple();
		String docName = args.getString("docName");
		String docContent = args.getString("docContent");
		boolean docFailed = args.getBoolean("docFailed");
		if (docFailed) {
			logger.info("Skipping pipelineStep: " + pipelineStep.getName() + " for document: " + docName + " because of previous failure...");
			result.add(new Text(docName));
			result.add(new Text(docContent));
			result.add(true);
		} else {
			try {
				Module instance = pipelineStep.getInstance();
				instance.setInputDocument(docContent);
				instance.setLocalDirectory(localDir);
				long tstart = System.currentTimeMillis();
				FutureTask<Module> executeModule = mes.executeModule(instance);
				Module outputInstance = executeModule.get(pipelineStep.getTimeout(), TimeUnit.MILLISECONDS);
				String outputDocument = outputInstance.getOutputDocument();
				boolean outputDocFailed = outputInstance.hasFailed();
				long tend = System.currentTimeMillis();
				logger.info("Applying pipelineStep: " + pipelineStep.getName() + " on document: " + docName + " took " + (tend - tstart) + " ms.");
				logger.info("Module " + pipelineStep.getName() + " result: " + !outputDocFailed + " on document: " + docName);
				result.add(new Text(docName));
				result.add(new Text(outputDocument));
				result.add(outputDocFailed);
			} catch (Exception e) {
				result.add(new Text(docName));
				result.add(new Text(docContent));
				result.add(true);
				logger.error(e);
			}
		}
		return result;
	}
}
