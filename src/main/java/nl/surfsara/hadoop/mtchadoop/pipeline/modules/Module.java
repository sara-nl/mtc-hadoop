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

import java.util.concurrent.Callable;

/**
 * Defines pipeline module functionality.
 *
 * @author mathijs.kattenberg@surfsara.nl
 */
public interface Module extends Callable<Module> {

    public abstract void setInputDocument(byte[] bytes);

    public abstract byte[] getInputDocument();

    public abstract void setOutputDocument(byte[] bytes);

    public abstract byte[] getOutputDocument();

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
     * @return A path to a directory that can be used for temporary data.
     */
    public abstract String getLocalDirectory();

    public abstract void setDocumentKey(String docName);

    public abstract String getDocumentKey();

}
