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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * An implementation of a the FBK-time Newsreader NLP component.
 * 
 * Most Newsreader components run as a run.sh bash script that consumes a NAF
 * text from standard in and outputs the annotated NAF text to standard out. For
 * running on Hadoop two default arguments have been added: the absolute path to
 * the component directory and an absolute path to a directory usable as scratch
 * or temporary storage. In addition FBK-time needs the document to be stored on
 * local scratch and is provided with this file as one of the arguments.
 * 
 * This class sets up the input- and outputstreams and calls the run.sh script
 * with the correct arguments. Failures are flagged due to timeout (failure to
 * process in time) or by exceeding a threshold of newlines in the standard
 * error stream (see the PipelineStep class for these settings).
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class NAFFileProvidedNewsreaderModule extends SubprocessModule {
	private static final Logger logger = Logger.getLogger(NAFFileProvidedNewsreaderModule.class);

	private PipelineStep pipelineStep;

	public NAFFileProvidedNewsreaderModule(PipelineStep step) {
		this.pipelineStep = step;
	}

	@Override
	public Module call() throws Exception {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ByteArrayOutputStream bes = new ByteArrayOutputStream();

		String filename = UUID.randomUUID().toString();
		File xmlf = new File(getLocalDirectory() + "/" + filename);
		FileOutputStream fos = new FileOutputStream(xmlf);
		InputStream xmlStream = IOUtils.toInputStream(getInputDocument(), "UTF-8");
		IOUtils.copy(xmlStream, fos);
		fos.flush();
		fos.close();
		xmlStream.close();

		File f = new File(pipelineStep.getModulePath() + "/run.sh.hadoop");
		File component = new File(pipelineStep.getModulePath());
		File scratch = new File(getLocalDirectory());

		super.setCommandLine("/bin/bash " + f.getAbsolutePath() + " " + component.getAbsolutePath() + "/ " + scratch.getAbsolutePath() + "/ " + xmlf.getAbsolutePath());

		super.setSubProcessStdIn(IOUtils.toInputStream(getInputDocument()));
		super.setSubProcessStdOut(bos);
		super.setSubProcessStdErr(bes);
		super.runSubprocess();

		String stderr = bes.toString();
		int newlines = stderr.split(System.getProperty("line.separator")).length;
		if (newlines > pipelineStep.getNumErrorLines()) {
			setFailed(true);
		}
		logger.error(stderr);
		bos.flush();
		setOutputDocument(bos.toString());
		bos.close();
		bes.close();
		xmlf.delete();
		return this;
	}
}
