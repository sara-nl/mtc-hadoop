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
package nl.surfsara.newsreader.pipeline.modules;

import java.util.concurrent.Callable;

/**
 * Defines a Newsreader NLP modules functionality.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public interface Module extends Callable<Module> {

	/**
	 * Set the input document to the provided text
	 * 
	 * @param xmlText
	 *            the text of the document to process
	 */
	public abstract void setInputDocument(String xmlText);

	/**
	 * Gets the current input
	 * 
	 * @return the current NAF document
	 */
	public abstract String getInputDocument();

	/**
	 * Set the output document to the specified text
	 * 
	 * @param xmlText
	 *            the text of the output document
	 */
	public abstract void setOutputDocument(String xmlText);

	/**
	 * Get the output text
	 * 
	 * @return the output or null if there is no output defined
	 */
	public abstract String getOutputDocument();

	/**
	 * Check whether processing has failed
	 * 
	 * @return true when the module failed to execute correctly (possibly in earlier modules)
	 */
	public abstract boolean hasFailed();

	/**
	 * Set the failed field. 
	 * 
	 * @param failed true when processing has failed.
	 */
	public abstract void setFailed(boolean failed);

	/** 
	 * Set the local scratch directory (unique for each task)
	 * 
	 * @param localDir A path to a directory that can be used for temporary data. 
	 */
	public abstract void setLocalDirectory(String localDir);

	/**
	 * Gets the local scratch directory
	 * 
	 * @return  A path to a directory that can be used for temporary data.
	 */
	public abstract String getLocalDirectory();
}
