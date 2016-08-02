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

import org.apache.hadoop.io.Text;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Function that strips the document failed (docFailed) field from the stream when it is no longer needed. 
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
@SuppressWarnings("serial")
public class StripField extends BaseOperation<Tuple> implements Function<Tuple> {
	
	// Eats: <docName, docContent, docFailed>
	// Emits: <docName, docContent>
	public StripField() {
		super(3, new Fields("docName", "docContent"));
	}

	public StripField(Fields fields) {
		super(3, fields);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
		TupleEntry args = functionCall.getArguments();
		String docName = args.getString("docName");
		String docContent = args.getString("docContent");
		Tuple result = new Tuple();
		result.add(new Text(docName));
		result.add(new Text(docContent));
		functionCall.getOutputCollector().add(result);
	}

}
