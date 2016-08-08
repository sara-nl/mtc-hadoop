/**
 * Copyright 2016 SURFsara
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.surfsara.hadoop.mtchadoop.pipeline.modules;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * An implementation of a bash pipeline component.
 * <p/>
 * This module writes the input data to a scratch directory on the local node. Runs a bash script and reads output
 * from a predefined outputdirectory. This class sets up the input- and outputstreams and
 * calls the run.sh script with the useful arguments. Failures are flagged due
 * to timeout (failure to process in time) or by exceeding a threshold of
 * newlines in the standard error stream (see the PipelineStep class for these
 * settings).
 *
 * @author mathijs.kattenberg@surfsara.nl
 */
public class GenericBashModule extends SubprocessModule {
    private static final Logger logger = Logger.getLogger(GenericBashModule.class);

    private PipelineStep pipelineStep;

    public GenericBashModule(PipelineStep step) {
        this.pipelineStep = step;
    }

    @Override
    public Module call() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ByteArrayOutputStream bes = new ByteArrayOutputStream();

        File f = new File(pipelineStep.getModulePath() + "/run.sh");
        File component = new File(pipelineStep.getModulePath());
        File scratch = new File(getLocalDirectory());

        // Write input to scratch
        InputStream is = new ByteArrayInputStream(getInputDocument());
        File iDir = new File(scratch + "/input/");
        iDir.mkdirs();
        File iFile = new File(iDir, getDocumentKey());
        FileOutputStream fos = new FileOutputStream(iFile);
        IOUtils.copyLarge(is, fos);
        fos.flush();
        fos.close();

        // Run script
        super.setCommandLine("/bin/bash " + f.getAbsolutePath() + " " + getDocumentKey() + " " + component.getAbsolutePath() + "/ " + scratch.getAbsolutePath() + "/");
        super.setSubProcessStdOut(bos);
        super.setSubProcessStdErr(bes);
        int subReturn = super.runSubprocess();
        if (subReturn != 0) {
            setFailed(true);
        }

        String stderr = bes.toString();
        int newlines = stderr.split(System.getProperty("line.separator")).length;
        if (newlines > pipelineStep.getNumErrorLines()) {
            setFailed(true);
        }
        String stdout = bos.toString();
        logger.info(stdout);
        logger.error(stderr);
        bos.flush();
        bos.close();
        bes.close();

        // Read output file from scratch
        File outputFile = new File(scratch + "/output/", getDocumentKey());
        if (outputFile.exists()) {
            FileInputStream fis = new FileInputStream(outputFile);
            setOutputDocument(IOUtils.toByteArray(fis));
            fis.close();
        } else {
            logger.info("Output file does not exist - failing this module.");
            setFailed(true);
            setOutputDocument(getInputDocument());
        }
        return this;
    }

}
