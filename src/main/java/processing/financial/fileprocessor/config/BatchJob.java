package processing.financial.fileprocessor.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.*;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import processing.financial.fileprocessor.batch.CustomPartitioner;
import processing.financial.fileprocessor.batch.FileSplitterTasklet;
import processing.financial.fileprocessor.batch.TransactionItemProcessor;
import processing.financial.fileprocessor.domain.Transaction;
import processing.financial.fileprocessor.domain.TransactionEnriched;
import javax.sql.DataSource;
import java.io.IOException;
import java.net.MalformedURLException;

@EnableBatchProcessing
@Configuration
@Slf4j
public class BatchJob {

    @Autowired
    ResourcePatternResolver resoursePatternResolver;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private NamedParameterJdbcTemplate namedJdbcTemplate;

   @Autowired
   JobRepository jobRepository;

   @Autowired
   JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;


    @Bean
    public JobBuilder jobBuilder(){
        return new JobBuilder("Jobs");
    }

    @Bean
    public StepBuilder stepBuilder(){
        return new StepBuilder("Steps");
    };


    @Bean(name = "transformFileItems")
    public Job transformFileItems(Step partitionStep, Step splitFile)  throws UnexpectedInputException,  ParseException {
        return jobBuilderFactory.get("partitionerJob")
                .start(splitFile)
                .next(partitionStep)
                .build();
    }

    @Bean
    public Step partitionStep(FlatFileItemReader flatFileItemReader, TransactionItemProcessor transactionItemProcessor,ItemWriter<TransactionEnriched> batchJdbcItemWriter) throws UnexpectedInputException, MalformedURLException, ParseException {
        return stepBuilderFactory.get("partitionStep")
                .partitioner("slaveStep", partitioner())
                .step(slaveStep(flatFileItemReader,transactionItemProcessor,batchJdbcItemWriter))
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Step slaveStep(FlatFileItemReader flatFileItemReader, TransactionItemProcessor transactionItemProcessor,ItemWriter<TransactionEnriched> batchJdbcItemWriter) throws UnexpectedInputException, ParseException {
        return stepBuilderFactory.get("slaveStep")
                .<Transaction, TransactionEnriched>chunk(5000)
                .reader(flatFileItemReader)
                .processor(transactionItemProcessor)
                .writer(batchJdbcItemWriter)
                .build();
    }

    @Bean
    public TransactionItemProcessor transactionItemProcessor(){
        return new TransactionItemProcessor();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Transaction> flatItemReader(@Value("#{stepExecutionContext[fileName]}") String input) throws MalformedURLException {
        FlatFileItemReaderBuilder<Transaction> builder = new FlatFileItemReaderBuilder<>();
        log.info("Filename: " + input);
        BeanWrapperFieldSetMapper<Transaction> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        log.info("Configuring reader to input {}", input);
        return builder
                .name("flatItemReader")
                .resource(new UrlResource(input))
                .lineTokenizer(fixedLengthTokenizer())
                .fieldSetMapper(fieldSetMapper)
                .targetType(Transaction.class)
                .build();
    }

    @Bean
    public FixedLengthTokenizer fixedLengthTokenizer() {
        FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();

        tokenizer.setNames("accountNumber", "amount");
        tokenizer.setColumns(new Range(1,24),
                new Range(25,41)
        );

        return tokenizer;
    }

    @Bean
    public CustomPartitioner partitioner() {
        CustomPartitioner partitioner = new CustomPartitioner();
        Resource[] resources;
        try {
            resources = resoursePatternResolver.getResources("file:input/input_split/*.file");
        } catch (IOException e) {
            throw new RuntimeException("I/O problems when resolving the input file pattern.",  e);
        }
        partitioner.setResources(resources);
        return partitioner;
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(20);
        taskExecutor.setCorePoolSize(20);
        taskExecutor.setQueueCapacity(20);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    private static final String QUERY_INSERT_TRANSACTION = "INSERT " +
            "INTO FILE_ITEMS(recordUniqueID,batchID,accountNumber,itemStatus,payload) " +
            "VALUES (:recordUniqueID,:batchID,:accountNumber,:itemStatus,:payload)";

    @Bean
    @StepScope
    ItemWriter<TransactionEnriched> batchJdbcItemWriter() {

        JdbcBatchItemWriter<TransactionEnriched> databaseItemWriter = new JdbcBatchItemWriter<>();
        databaseItemWriter.setDataSource(dataSource);
        databaseItemWriter.setJdbcTemplate(namedJdbcTemplate);
        ItemSqlParameterSourceProvider<TransactionEnriched> itemSqlParameterSourceProvider = new BeanPropertyItemSqlParameterSourceProvider<>();
        databaseItemWriter.setItemSqlParameterSourceProvider(itemSqlParameterSourceProvider);

        databaseItemWriter.setSql(QUERY_INSERT_TRANSACTION);

        return databaseItemWriter;
    }
    @Bean
    public Step splitFile() {
        return stepBuilderFactory.get("splitFile")
                .tasklet(new FileSplitterTasklet())
                .build();
    }
}
