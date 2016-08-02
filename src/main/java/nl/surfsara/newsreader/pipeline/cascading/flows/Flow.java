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

import cascading.flow.FlowDef;

/**
 * Defines a Flow interface for the Newsreader pipeline.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public interface Flow {

	/**
	 * Create a Cascading FlowDef for the specified parameters
	 * 
	 * @param inPath
	 *            A path to be used as input for the flow
	 * @param outPath
	 *            A path to be used as output for the floe
	 * @param errorPath
	 *            A path where failed documents will be stored
	 * @return A Flowdefinition
	 * @throws Exception On any error during setup of the flow.
	 */
	public abstract FlowDef getFlowDefinition(String inPath, String outPath, String errorPath) throws Exception;
}
