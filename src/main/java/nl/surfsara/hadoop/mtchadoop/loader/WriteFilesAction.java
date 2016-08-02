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
package nl.surfsara.hadoop.mtchadoop.loader;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Accessing HDFS needs to be performed with privileges for a principal (user)
 * enabled. This is an implementation of a PriviligedAction that, as the logged
 * in user, writes files from the local file system to one ore more sequence files.
 *
 * @author mathijs.kattenberg@surfsara.nl
 */
public class WriteFilesAction implements PrivilegedAction<Long> {

    private static final Logger logger = Logger.getLogger(WriteFilesAction.class);

    private File file;
    private SequenceFile.Writer writer;
    private String destination;
    private int docsPerFile;

    private Configuration conf;

    public WriteFilesAction(Configuration conf, String source, String destination, int docsPerFile) throws IOException {
        this.conf = conf;
        file = new File(source);
        this.destination = destination;
        this.docsPerFile = docsPerFile;
        initWriter(conf, destination + "_0");
    }

    private void initWriter(Configuration conf, String path) throws IOException {
        CompressionCodec Codec = new DefaultCodec();
        writer = null;
        Option optPath = SequenceFile.Writer.file(new Path(path));
        Option optKey = SequenceFile.Writer.keyClass(Text.class);
        Option optVal = SequenceFile.Writer.valueClass(BytesWritable.class);
        Option optCom = SequenceFile.Writer.compression(CompressionType.BLOCK, Codec);
        writer = SequenceFile.createWriter(conf, optPath, optKey, optVal, optCom);
    }

    @Override
    public Long run() {
        long filesAppended = 0;
        int numFile = 0;
        List<File> files = null;
        try {
            if (validate(file)) {
                if (file.isDirectory()) {
                    files = getFileListing(file);
                } else if (file.isFile()) {
                    files = new ArrayList<File>();
                    files.add(file);
                }
                for (File f : files) {
                    String name = f.getName();
                    byte[] content = readContent(f);
                    if (appendDoc(name, content)) {
                        filesAppended++;
                    }
                    if (docsPerFile > 0) {
                        if (filesAppended % docsPerFile == 0) {
                            numFile++;
                            writer.hflush();
                            writer.hsync();
                            writer.close();
                            initWriter(conf, destination + "_" + numFile);
                        }
                    }
                }
                writer.hflush();
                writer.hsync();
                writer.close();
            }
        } catch (Exception e) {
            logger.error(e);
        }
        return filesAppended;
    }

    private byte[] readContent(File f) throws IOException {
        FileInputStream fis = new FileInputStream(f);
        byte[] result =  IOUtils.toByteArray(fis);
        fis.close();
        return result;
    }

    private List<File> getFileListing(File dir) throws FileNotFoundException {
        List<File> result = new ArrayList<File>();
        File[] filesAndDirs = dir.listFiles();
        List<File> filesDirs = Arrays.asList(filesAndDirs);
        for (File file : filesDirs) {
            result.add(file);
            if (!file.isFile()) {
                List<File> subList = getFileListing(file);
                result.addAll(subList);
            }
        }
        return result;
    }

    private boolean validate(File f) throws FileNotFoundException {
        if (f == null) {
            throw new IllegalArgumentException("File should not be null.");
        }
        if (!f.exists()) {
            throw new FileNotFoundException("File does not exist: " + f);
        }
        if (!f.canRead()) {
            throw new IllegalArgumentException("File cannot be read: " + f);
        }
        if (!f.isDirectory()) {
            throw new IllegalArgumentException("File should be a directory: " + f);
        }
        return true;
    }

    private boolean appendDoc(String docName, byte[] docContents) {
        try {
            writer.append(new Text(docName), new BytesWritable(docContents));
            writer.hflush();
            return true;
        } catch (IOException e) {
            logger.error(e);
            return false;
        }
    }

}
