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
package nl.surfsara.hadoop.mtchadoop.pipeline.flows.udfs;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Filter that removes files flagged as failed from the tuple stream.
 *
 * @author mathijs.kattenberg@surfsara.nl
 */
@SuppressWarnings({"serial"})
public class FailedFilter extends BaseOperation<Tuple> implements Filter<Tuple> {

    @SuppressWarnings("rawtypes")
    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        TupleEntry args = filterCall.getArguments();
        return args.getBoolean("docFailed");
    }

}
