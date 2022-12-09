package processing.financial.fileprocessor.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import processing.financial.fileprocessor.domain.Transaction;
import processing.financial.fileprocessor.domain.TransactionEnriched;
import java.util.UUID;

@Slf4j
public class TransactionItemProcessor implements ItemProcessor<Transaction, TransactionEnriched>
{
    private String fileName;

    @Autowired
    ObjectMapper objectMapper;

    @Bean
    @StepScope
    public TransactionEnriched process(Transaction trasaction) throws Exception
    {
        UUID id = UUID.randomUUID();
        String payload =  objectMapper.writeValueAsString(trasaction);
        log.info("File item: {}",payload);
        return new TransactionEnriched(id,fileName,trasaction.getAccountNumber(),"LOADED",payload);
    }

    @BeforeStep
    public void beforeStep(final StepExecution stepExecution) {
        JobParameters jobParameters = stepExecution.getJobParameters();
        fileName = jobParameters.getString("file.input");
    }
}