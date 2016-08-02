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

/**
 * Abstract implementation of a Module. A large part of the functionality is
 * common for all modules and implemented here.
 *
 * @author mathijs.kattenberg@surfsara.nl
 */
public abstract class AbstractModule implements Module {
    private String fileKey;
    private byte[] inputFileContents;
    private byte[] outputFileContents;
    private String localDir;
    private boolean docFailed = false;

    public void setInputDocument(byte[] input) {
        this.inputFileContents = input;
    }

    public byte[] getInputDocument() {
        return inputFileContents;
    }

    public void setOutputDocument(byte[] output) {
        this.outputFileContents = output;
    }

    public byte[] getOutputDocument() {
        return outputFileContents;
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

    public void setDocumentKey(String key) {
        this.fileKey = key;
    }

    public String getDocumentKey() {
        return fileKey;
    }
}
