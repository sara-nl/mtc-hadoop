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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * This class provides access to the Threadpool used to execute modules.
 *
 * @author mathijs.kattenberg@surfsara.nl
 */
public class ModuleExecutorService {
    private ExecutorService threadPool;

    public ModuleExecutorService() {
        threadPool = Executors.newCachedThreadPool();
    }

    public FutureTask<Module> executeModule(Module m) {
        FutureTask<Module> mft = new FutureTask<Module>(m);
        threadPool.execute(mft);
        return mft;
    }

    public void destroy() {
        threadPool.shutdownNow();
    }
}
