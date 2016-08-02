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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.StringTokenizer;

import nl.surfsara.newsreader.pipeline.util.PipeThread;

import org.apache.log4j.Logger;

/**
 * An abstract module that runs in a subprocess. Most modules will run in a
 * subprocess of some sort. This functionality is provided here.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public abstract class SubprocessModule extends AbstractModule {
	private static final Logger logger = Logger.getLogger(SubprocessModule.class);
	private InputStream subProcessStdIn = System.in;
	private OutputStream subProcessStdOut = System.out;
	private OutputStream subProcessStdErr = System.err;
	private String commandLine = "";

	public void setCommandLine(String value) {
		this.commandLine = value;
	}

	public String getCommandLine() {
		return commandLine;
	}

	public InputStream getSubProcessStdIn() {
		return subProcessStdIn;
	}

	public void setSubProcessStdIn(InputStream subProcessStdIn) {
		this.subProcessStdIn = subProcessStdIn;
	}

	public OutputStream getSubProcessStdOut() {
		return subProcessStdOut;
	}

	public void setSubProcessStdOut(OutputStream subProcessStdOut) {
		this.subProcessStdOut = subProcessStdOut;
	}

	public OutputStream getSubProcessStdErr() {
		return subProcessStdErr;
	}

	public void setSubProcessStdErr(OutputStream subProcessStdErr) {
		this.subProcessStdErr = subProcessStdErr;
	}

	public int runSubprocess() throws Exception {

		String commandLine = getCommandLine();
		logger.info("Running commandline: " + commandLine);
		StringTokenizer st = new StringTokenizer(commandLine, " ");
		ArrayList<String> argumentList = new ArrayList<String>();
		while (st.hasMoreTokens()) {
			argumentList.add(st.nextToken());
		}
		ProcessBuilder pb = new ProcessBuilder(argumentList);
		Process p;
		p = pb.start();

		Thread subIn = null;
		if (subProcessStdIn != null) {
			subIn = new Thread(new PipeThread("stdin", subProcessStdIn, p.getOutputStream(), true));
			subIn.start();
		}

		Thread subOut = null;
		if (subProcessStdOut != null) {
			if (subProcessStdOut.equals(System.out)) {
				subOut = new Thread(new PipeThread("stdout", p.getInputStream(), subProcessStdOut, false));
			} else {
				subOut = new Thread(new PipeThread("stdout", p.getInputStream(), subProcessStdOut, true));
			}
			subOut.start();
		}
		Thread subErr = null;
		if (subProcessStdErr != null) {
			if (subProcessStdErr.equals(System.err)) {
				subErr = new Thread(new PipeThread("stdout", p.getErrorStream(), subProcessStdErr, false));
			} else {
				subErr = new Thread(new PipeThread("stdout", p.getErrorStream(), subProcessStdErr, true));
			}
			subErr.start();
		}
		p.waitFor();

		if (subErr != null) {
			if (subErr.isAlive()) {
				subErr.join();
			}
			subErr = null;
		}

		if (subOut != null) {
			if (subOut.isAlive()) {
				subOut.join();
			}
			subOut = null;
		}

		if (subIn != null) {
			if (subIn.isAlive()) {
				subIn.join();
			}
			subIn = null;
		}
		return p.exitValue();
	}

}
