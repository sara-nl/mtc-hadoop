/**
 * Copyright 2016 SURFsara
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
package nl.surfsara.hadoop.mtchadoop;

import java.util.Arrays;
import java.util.StringTokenizer;

import nl.surfsara.hadoop.mtchadoop.loader.Loader;
import nl.surfsara.newsreader.pipeline.Pipeline;

/**
 * Main entry point for the mtc-hadoop tools.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class Main {
	public enum Tools {
		LOADER("loader", "Import/Export tool for files on HDFS."), PIPELINE("pipeline", "Run a pipeline on Hadoop.");

		private final String name;
		private final String description;

		private Tools(String name, String description) {
			this.name = name;
			this.description = description;
		}

		public String getName() {
			return name;
		}

		public String getDescription() {
			return description;
		}
	}

	public static void main(String[] args) {
		System.out.println("mtc-hadoop");
		int retval = 0;
		boolean showUsage = false;
		if (args.length <= 0) {
			showUsage();
			System.exit(0);
		}
		String tool = args[0];
		String[] toolArgs = Arrays.copyOfRange(args, 1, args.length);
		try {
			if (Tools.LOADER.getName().equals(tool)) {
				Loader l = new Loader(toolArgs);
				l.run();
			} else if (Tools.PIPELINE.getName().equals(tool)) {
				Pipeline p = new Pipeline(toolArgs);
				p.run();
			} else {
				showUsage = true;
			}
			if (showUsage) {
				showUsage();
			}
		} catch (Exception e) {
			showErrorAndExit(e);
		}
		System.exit(retval);
	}

	private static void showErrorAndExit(Exception e) {
		System.out.println("Something didn't quite work like expected: [" + e.getMessage() + "]");
		showUsage();
		System.exit(1);
	}

	private static void showUsage() {
		System.out.println("A tool must be given as the first argument followed by tool and/or Hadoop specific options.");
		System.out.println("Valid tool names are:");
		for (Tools prog : Tools.values()) {
			System.out.println(" " + prog.getName() + ": " + prog.getDescription());
		}
		System.out.println("Running a tool without arguments provides instructions for use.");
	}
}
