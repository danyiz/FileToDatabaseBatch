package processing.financial.fileprocessor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@Slf4j
@EnableAutoConfiguration
@PropertySource(name = "JobProperties", value = "values.properties")
public class FileProcessorApplication implements CommandLineRunner {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	@Qualifier("transformFileItems")
	private Job transformFileItems;

	@Value("${file.input}")
	private String input;


	public static void main(String[] args) {

		ConfigurableApplicationContext contex =  SpringApplication.run(FileProcessorApplication.class, args);
		contex.close();
	}

	@Override
	public void run(String... args) throws Exception {
		JobParametersBuilder paramsBuilder = new JobParametersBuilder();
		paramsBuilder.addString("inputFile", input);
		jobLauncher.run(transformFileItems, paramsBuilder.toJobParameters());
	}
}
