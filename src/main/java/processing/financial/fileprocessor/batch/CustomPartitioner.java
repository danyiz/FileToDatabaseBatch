

/*
 * Copyright 2006-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package processing.financial.fileprocessor.batch;


import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import processing.financial.fileprocessor.domain.exception.CreateDirsFailException;
import processing.financial.fileprocessor.domain.FileSplitter;
import processing.financial.fileprocessor.domain.exception.LineLengthException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Component
public class CustomPartitioner  implements Partitioner {

    @Autowired
    ResourcePatternResolver resourcePatternResolver;

    private static final String DEFAULT_KEY_NAME = "fileName";

    private static final String PARTITION_KEY = "partition";

    private Resource[] resources = new Resource[0];

    private String keyName = DEFAULT_KEY_NAME;

    @Bean
    @StepScope
    public CustomPartitioner partitioner(@Value("#{jobParameters[inputFile]}")String input) throws CreateDirsFailException, IOException, LineLengthException {

        var fileList = FileSplitter.splitTextFiles(input, 30000,41);

        Resource[] resources;
        try {
            resources = resourcePatternResolver.getResources("file:input/input_split/*.file");
        } catch (IOException e) {
            throw new RuntimeException("I/O problems when resolving the input file pattern.",  e);
        }
        this.setResources(resources);
        return this;
    }


    /**
     * The resources to assign to each partition. In Spring configuration you
     * can use a pattern to select multiple resources.
     * @param resources the resources to use
     */
    public void setResources(Resource[] resources) {
        this.resources = resources;
    }

    /**
     * The name of the key for the file name in each {@link ExecutionContext}.
     * Defaults to "fileName".
     * @param keyName the value of the key
     */
    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    /**
     * Assign the filename of each of the injected resources to an
     * {@link ExecutionContext}.
     *
     * @see Partitioner#partition(int)
     */
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> map = new HashMap<String, ExecutionContext>(gridSize);
        int i = 0;
        for (Resource resource : resources) {
            ExecutionContext context = new ExecutionContext();
            Assert.state(resource.exists(), "Resource does not exist: " + resource);
            //context.putString(keyName, resource.getFilename());
            try {
                String fileFullNameWithPath = resource.getURL().toString();
                context.putString(keyName, fileFullNameWithPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            map.put(PARTITION_KEY + i, context);
            i++;
        }
        return map;
    }
}