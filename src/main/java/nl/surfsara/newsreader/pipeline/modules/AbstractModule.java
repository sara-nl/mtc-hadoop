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

/**
 * Abstract implementation of a Module. A large part of the functionality is
 * common for all modules and implemented here.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public abstract class AbstractModule implements Module {
	private String xmlText;
	private String xmlOutput;
	private String localDir;
	private boolean docFailed = false;

	public void setInputDocument(String xmlText) {
		this.xmlText = xmlText;
	}

	public String getInputDocument() {
		return xmlText;
	}

	public void setOutputDocument(String xmlOutput) {
		this.xmlOutput = xmlOutput;
	}

	public String getOutputDocument() {
		return xmlOutput;
	}

	public void setLocalDirectory(String localDir) {
		this.localDir = localDir;
	}

	public String getLocalDirectory() {
		return localDir;
	}

	public boolean hasFailed() {
		return docFailed;
	}

	public void setFailed(boolean failed) {
		this.docFailed = failed;
	}

}
