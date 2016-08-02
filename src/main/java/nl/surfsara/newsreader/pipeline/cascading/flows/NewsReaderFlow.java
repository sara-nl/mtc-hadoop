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
package nl.surfsara.newsreader.pipeline.cascading.flows;

import nl.surfsara.newsreader.pipeline.PipelineLayout;
import nl.surfsara.newsreader.pipeline.cascading.flows.udfs.FailedFilter;
import nl.surfsara.newsreader.pipeline.cascading.flows.udfs.InsertField;
import nl.surfsara.newsreader.pipeline.cascading.flows.udfs.RunModuleFunction;
import nl.surfsara.newsreader.pipeline.cascading.flows.udfs.StripField;
import nl.surfsara.newsreader.pipeline.cascading.flows.udfs.SuccessFilter;
import nl.surfsara.newsreader.pipeline.modules.PipelineStep;

import org.apache.hadoop.io.Text;

import cascading.flow.FlowDef;
import cascading.pipe.Checkpoint;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * An implementation of the Newsreader NLP pipeline as a Cascading Flow.
 * 
 * The steps in the pipeline are described in the PipelineLayout class.
 * 
 * Documents are read from sequence files(s) on HDFS and inserted into the tuple
 * stream as the following tuples: <document name, document contents>. The first
 * function inserts the document failed field for each tuple: <document name,
 * document contents, document failed> The next functions execute the pipeline
 * on the document contents field. Documents where docFailed has been set to
 * true will not be processed by subsequent moduled. Finally, the stream is
 * split and failed and successful documents are stored in separate sinks on
 * HDFS (again as sequence files with <key,value> = <document name, document
 * contents>
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class NewsReaderFlow implements Flow {
	private PipelineLayout pl;

	public NewsReaderFlow(PipelineLayout pl) {
		this.pl = pl;
	}

	@Override
	public FlowDef getFlowDefinition(String inPath, String outPath, String errorPath) throws Exception {
		Fields pipelineFields = new Fields("docName", "docContent", "docFailed");
		Fields docFields = new Fields("docName", "docContent");

		//SequenceFile seq = new SequenceFile(docFields);
		WritableSequenceFile inseq = new WritableSequenceFile(docFields, Text.class, Text.class);

		@SuppressWarnings("rawtypes")
		Tap docTap = new Hfs(inseq, inPath);

		Pipe insertField = new Each("Insert docFailed", new InsertField(), Fields.RESULTS);

		// Build pipeline from steps
		Pipe prevPipe = insertField;
		for (PipelineStep ps : pl.getSteps()) {
			Pipe currentPipe = new Each(new Pipe(ps.getName(), prevPipe), pipelineFields, new RunModuleFunction(ps), Fields.RESULTS);
			prevPipe = currentPipe;
		}
		Checkpoint checkPoint = new Checkpoint("Checkpoint", prevPipe);

		Pipe succesDocs = new Each(new Pipe("Select succesful docs", checkPoint), pipelineFields, new FailedFilter());
		Pipe sstrip = new Each(new Pipe("Strip docFailed from succesful docs", succesDocs), pipelineFields, new StripField(), Fields.RESULTS);

		Pipe failedDocs = new Each(new Pipe("Select failed docs", checkPoint), pipelineFields, new SuccessFilter());
		Pipe fstrip = new Each(new Pipe("Strip docFailed from failed docs", failedDocs), pipelineFields, new StripField(), Fields.RESULTS);

		WritableSequenceFile outseq = new WritableSequenceFile(docFields, Text.class, Text.class);
		SequenceFile checkPointSeq = new SequenceFile(Fields.ALL);

		@SuppressWarnings("rawtypes")
		Tap successSink = new Hfs(outseq, outPath);
		@SuppressWarnings("rawtypes")
		Tap checkpointSink = new Hfs(checkPointSeq, outPath + "_checkpoint");
		@SuppressWarnings("rawtypes")
		Tap failedSink = new Hfs(outseq, errorPath);

		return FlowDef.flowDef().addSource(insertField, docTap).addCheckpoint(checkPoint, checkpointSink).addTailSink(sstrip, successSink).addTailSink(fstrip, failedSink);
	}

}
