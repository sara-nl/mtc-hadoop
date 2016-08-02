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
package nl.surfsara.newsreader.pipeline.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.OutputStream;

/**
 * Utility class; logs a stream to a log4j logger. 
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class LogOutputStream extends OutputStream {
    private Logger logger;
    private Level level;
    private String buffer;
    public LogOutputStream (Logger logger, Level level) {
        setLogger (logger);
        setLevel (level);
        buffer = "";
    }

    public void setLogger (Logger logger) {
        this.logger = logger;
    }

    public Logger getLogger () {
        return logger;
    }

    public void setLevel (Level level) {
        this.level = level;
    }

    public Level getLevel () {
        return level;
    }

    public void write (int b) {
        byte[] bytes = new byte[1];
        bytes[0] = (byte) (b & 0xff);
        buffer = buffer + new String(bytes);

        if (buffer.endsWith ("\n")) {
            buffer = buffer.substring (0, buffer.length () - 1);
            flush();
        }
    }

    public void flush () {
        logger.log (level, buffer);
        buffer = "";
    }
}