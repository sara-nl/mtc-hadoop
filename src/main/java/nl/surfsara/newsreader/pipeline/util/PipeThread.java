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
package nl.surfsara.newsreader.pipeline.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Runnable class that copies (pipes) an Inputstream to an Outputstream. Used
 * for streaming subprocess Input- and Outputstreams in a separate thread
 * (preventing deadlocks).
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class PipeThread implements Runnable {
	private static final Logger logger = Logger.getLogger(PipeThread.class);
	private OutputStream os;
	private InputStream is;
	private boolean closeAfterCopy;
	private String id;

	public PipeThread(String id, InputStream is, OutputStream os, boolean closeAfterCopy) {
		this.id = id;
		this.is = new BufferedInputStream(is);
		this.os = new BufferedOutputStream(os);
		this.closeAfterCopy = closeAfterCopy;
	}

	public void run() {
		try {
			IOUtils.copy(is, os);
			os.flush();
			if (closeAfterCopy) {
				os.close();
				is.close();
			}
		} catch (IOException e) {
			logger.error(id + ": " + e);
		}
	}

}
