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

import java.io.Serializable;
import java.lang.reflect.Constructor;

/**
 * Class that defines for each component in the pipeline its name, executing class,
 * timeout and allowable newlines in stderr.
 *
 * @author mathijs.kattenberg@surfsara.nl
 */
public class PipelineStep implements Serializable {
    /**
     * Serial version ID for version 1.1
     */
    private static final long serialVersionUID = -8824482494194734735L;

    private final String name;
    private final Class<? extends Module> c;
    private long timeout;
    private int numErrorLines;

    public PipelineStep(String name, Class<? extends Module> c, long timeout, int numErrorLines) {
        this.name = name;
        this.c = c;
        this.timeout = timeout;
        this.numErrorLines = numErrorLines;
    }

    public Module getInstance() throws Exception {
        Constructor<? extends Module> constructor = c.getConstructor(PipelineStep.class);
        return constructor.newInstance(this);
    }

    public String getName() {
        return name;
    }

    public String getModulePath() {
        return ModuleConstants.ARCHIVEROOT + "/" + getName();
    }

    public int getNumErrorLines() {
        return numErrorLines;
    }

    public long getTimeout() {
        return timeout;
    }

}
