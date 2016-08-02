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

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Runnable class that uses an existing Kerberos tgt to to upload to, or
 * download newsreader files from, HDFS.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class Loader implements Runnable {
	private static final Logger logger = Logger.getLogger(Loader.class);
	private String[] args;
	private UserGroupInformation loginUser;
	private Configuration conf;

	public Loader(String[] args) {
		this.args = args;
	}

	@Override
	public void run() {
		PropertyConfigurator.configure("log4jconfig.properties");
		boolean showusage = false;
		if (args.length < 3) {
			showusage = true;
		} else {
			String mode = args[0];
			String source = args[1];
			String dest = args[2];

			if ("load".equals(mode)) {
				if (args.length < 4) {
					showusage = true;
				} else {
					System.out.println("Uploading documents to Hadoop...");
					System.out.println("Using " + Charset.defaultCharset() + " as default encoding.");
					try {
						init();
						int docsPerFile = Integer.parseInt(args[3]);
						WriteNewsreaderDocs wnd = new WriteNewsreaderDocs(conf, source, dest, docsPerFile);
						Long filesWritten = loginUser.doAs(wnd);
						System.out.println("Wrote " + filesWritten + " documents from " + source + " to sequencefiles in " + dest + ".");
					} catch (IOException e) {
						logger.debug(e);
						System.out.println("Failed login to Hadoop: " + e.getMessage());
					} catch (NumberFormatException e) {
						logger.debug(e);
						System.out.println("Documents per file is not specified correctly: " + e.getMessage());
					}
				}
			} else if ("unload".equals(mode)) {
				System.out.println("Downloading documents from Hadoop...");
				try {
					init();
					ReadNewsreaderDocs rnd = new ReadNewsreaderDocs(conf, source, dest);
					Long filesRead = loginUser.doAs(rnd);
					System.out.println("Read " + filesRead + " documents from sequencefiles in " + source + " to " + dest + ".");
				} catch (IOException e) {
					logger.debug(e);
					System.out.println("Failed login to Hadoop: " + e.getMessage());
				} catch (NumberFormatException e) {
					logger.debug(e);
					System.out.println("Documents per file is not specified correctly: " + e.getMessage());
				}

			} else {
				showusage = true;
			}
		}
		if (showusage) {
			showUsage();
		}
	}

	private void showUsage() {
		System.out.println("Usage: ");
		System.out.println();
		System.out.println("The load program uploads files to, or downloads them from, Hadoop.");
		System.out.println("When uploading files are stored in one or more sequencefiles where the key is the document name and the value is the document content as text.");
		System.out.println("When downloading documents are read from one or more sequencefiles and stored into their respective documents.");
		System.out.println();
		System.out.println("The loader expects the following arguments: ");
		System.out.println(" 1.) a mode: can be one of 'load' or 'unload' to upload to, or download from, Hadoop respectively.");
		System.out.println(" 2.) a source: for download this should be a path on HDFS, for upload a path on the local filesystem.");
		System.out.println(" 3.) a destination: for download this should be a path on the local filesystem, for upload a path on HDFS.");
		System.out.println(" 4.) a documents per file setting (only for upload) the tool can distribute the files to one or more destination sequencefiles.");
		System.out.println("     Use a setting of -1 to write all documents to one file only."); 
		System.out.println();
		System.out.println("Some examples:");
		System.out.println("Upload /foo/* to a single file in /bar/file_0 on HDFS: java -jar newsreader-hadoop.jar loader load /foo /bar/file -1");
		System.out.println("Upload /foo/* to files in /bar/file/docs_{0..} on HDFS with 10 docs per file: java -jar newsreader-hadoop.jar loader load /foo /bar/file/docs 10");
		System.out.println("Note that in the last example the number of files on HDFS is dependent on the number of input files (e.g. for 100 xml files; 10 sequence");
		System.out.println("files will be created. The filename will be postfixed with a _ and filenumber). This also effects the amount of mappers.");
		System.out.println();
		System.out.println("Download /foo/part* on HDFS to local document files in /bar/: java -jar newsreader-hadoop.jar loader load /foo/part* /bar/");
		System.out.println();
	}

	private void init() throws IOException {
		conf = new Configuration();
		conf.addResource(new Path("core-site.xml"));
		conf.addResource(new Path("hdfs-site.xml"));

		conf.set("hadoop.security.authentication", "kerberos");
		conf.set("hadoop.security.authorization", "true");

		System.setProperty("java.security.krb5.realm", "CUA.SURFSARA.NL");
		System.setProperty("java.security.krb5.kdc", "kdc.hathi.surfsara.nl");

		UserGroupInformation.setConfiguration(conf);

		loginUser = UserGroupInformation.getLoginUser();
		logger.info("Logged in as: " + loginUser.getUserName());
	}
}
