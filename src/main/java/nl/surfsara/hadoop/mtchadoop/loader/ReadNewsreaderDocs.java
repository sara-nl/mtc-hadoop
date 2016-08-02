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
package nl.surfsara.hadoop.mtchadoop.loader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.security.PrivilegedAction;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;

/**
 * Accessing HDFS needs to be performed with privileges for a principal (user)
 * enabled. This is an implementation of a PriviligedAction that, as the logged
 * in user, reads NAF documents from a sequence file and stores these on the
 * local file system.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class ReadNewsreaderDocs implements PrivilegedAction<Long> {

	private Configuration conf;
	private String source;
	private String dest;

	public ReadNewsreaderDocs(Configuration conf, String source, String dest) {
		this.conf = conf;
		this.source = source;
		this.dest = dest;
	}

	@Override
	public Long run() {
		long numfilesread = 0;
		Path sPath = new Path(source);
		File destDir = new File(dest);
		if (destDir.isDirectory()) {
			destDir.mkdirs();
			try {
				FileSystem fileSystem = FileSystem.get(conf);
				FileStatus[] globStatus = fileSystem.globStatus(sPath);
				for (FileStatus fss : globStatus) {
					if (fss.isFile()) {
						Option optPath = SequenceFile.Reader.file(fss.getPath());
						SequenceFile.Reader r = new SequenceFile.Reader(conf, optPath);

						Text key = new Text();
						Text val = new Text();

						while (r.next(key, val)) {
							File outputFile = new File(destDir, key.toString());
							FileOutputStream fos = new FileOutputStream(outputFile);
							InputStream is = IOUtils.toInputStream(val.toString());
							IOUtils.copy(is, fos);
							fos.flush();
							fos.close();
							numfilesread++;
						}
						r.close();
					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("Destination should be a directory.");
		}
		return numfilesread;
	}

}
