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
package nl.surfsara.hadoop.mtchadoop.pipeline;

import nl.surfsara.hadoop.mtchadoop.pipeline.modules.Module;
import nl.surfsara.hadoop.mtchadoop.pipeline.modules.PipelineStep;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that reads and parses a layout file. The layout file is json document
 * that describes the steps in the pipeline to execute. Please see the json
 * schema for more details on this file.
 *
 * @author mathijs.kattenberg@surfsara.nl
 */
public class PipelineLayout {
    private String pipelineversion;
    private String pipelineid;
    private String description;
    private ArrayList<PipelineStep> steps;
    private String layoutFile;

    public PipelineLayout(String layoutFile) throws FileNotFoundException, IOException, ClassNotFoundException {
        this.layoutFile = layoutFile;
        init(layoutFile);
    }

    private void init(String layouFile) throws FileNotFoundException, IOException, ClassNotFoundException {
        List<String> lines = IOUtils.readLines(new FileReader(new File(layoutFile)));
        StringBuilder jsonString = new StringBuilder();
        for (String s : lines) {
            jsonString.append(s);
        }
        // TODO move field reference strings to constants/enums or a static class
        JSONObject jo = new JSONObject(jsonString.toString());
        pipelineid = (String) jo.get("id");
        pipelineversion = (String) jo.get("version");
        description = (String) jo.get("description");
        JSONArray ja = jo.getJSONArray("layout");
        steps = new ArrayList<PipelineStep>();
        for (int i = 0; i < ja.length(); i++) {
            JSONObject jsonObject = ja.getJSONObject(i);
            String name = jsonObject.getString("name");
            String className = jsonObject.getString("class");
            long timeOut = jsonObject.getLong("timeout");
            int numErrorLine = jsonObject.getInt("numErrorLines");

            @SuppressWarnings("unchecked")
            PipelineStep step = new PipelineStep(name, ((Class<? extends Module>) Class.forName(className)), timeOut, numErrorLine);
            steps.add(step);
        }
    }

    public String getPipelineversion() {
        return pipelineversion;
    }

    public String getDescription() {
        return description;
    }

    public ArrayList<PipelineStep> getSteps() {
        return steps;
    }

    public String getPipelineid() {
        return pipelineid;
    }

}
