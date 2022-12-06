package processing.financial.fileprocessor.batch;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import processing.financial.fileprocessor.domain.FileSplitter;
import java.util.List;

@Slf4j
@NoArgsConstructor
public class FileSplitterTasklet implements Tasklet, StepExecutionListener {

    private String fileName;
    List<String> splitFileList = null;

    @Override
    @StepScope
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        splitFileList= FileSplitter.splitTextFiles(fileName, 100000,41);
        log.info("File was split on {} files", splitFileList.size());
        splitFileList.forEach(s -> log.info("File  {} ", s));

        return RepeatStatus.FINISHED;
   }

    @BeforeStep
    public void beforeStep(final StepExecution stepExecution) {
        JobParameters jobParameters = stepExecution.getJobParameters();
        fileName = jobParameters.getString("file.input");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        ExecutionContext jobContext = stepExecution.getJobExecution().getExecutionContext();
        splitFileList.forEach(s -> jobContext.put("File"+s, s));
        return ExitStatus.COMPLETED;
    }
}
