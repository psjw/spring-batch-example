package com.spring.batch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

@Configuration
@Slf4j
public class ChunkProcessingSpringExpressionLangConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public ChunkProcessingSpringExpressionLangConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job chunkProcessingSpringExpressionLangJob(){
        return jobBuilderFactory.get("chunkProcessingSpringExpressionLangJob")
                .incrementer(new RunIdIncrementer())
                .start(this.taskBaseSpringExpressionLangStep())
                .next(this.chunkBaseSpringExpressionLangStep(null)) //null로 설정해도 문제없음 -> @JobScope때문
                .build();
    }

    @Bean
    public Step taskBaseSpringExpressionLangStep() {
      return stepBuilderFactory.get("taskBaseSpringExpressionLangStep")
              .tasklet(this.tasklet())
              .build();
    }

    @Bean
    @JobScope
    public Step chunkBaseSpringExpressionLangStep(@Value("#{jobParameters[chunkSize]}") String chunkSize){
        return stepBuilderFactory.get("chunkBaseSpringExpressionLangStep")
                //처음 String : input type , 두번째 String : outputType
                .<String, String>chunk(StringUtils.hasLength(chunkSize) ? Integer.parseInt(chunkSize) : 10) //100개의 데이터를 10개씩 나눔
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    private ItemWriter<String> itemWriter() {
        return items -> log.info("chunk item size : {}", items.size());
//        return items -> items.forEach(log::info);
    }

    private ItemProcessor<String, String> itemProcessor() {
        //null로 리턴되면 itemWriter로 안넘어감
        return item -> item + ",  Spring Batch";
    }

    private ItemReader<String> itemReader() {
        return new ListItemReader<>(getItems());
    }

    private Tasklet tasklet() {
        List<String> items = getItems();
        return (contribution, chunkContext) -> {
            StepExecution stepExecution = contribution.getStepExecution();
            JobParameters jobParameters = stepExecution.getJobParameters();

            String value = jobParameters.getString("chunkSize", "10");
            int chunkSize = StringUtils.hasLength(value) ? Integer.parseInt(value) : 10;

            int fromIndex = stepExecution.getReadCount();
            int toIndex = fromIndex + chunkSize;

            if(fromIndex >=  items.size()){
                return RepeatStatus.FINISHED;
            }

            List<String> subList = items.subList(fromIndex, toIndex);


//            log.info("task item size : {}",items.size());
            log.info("task item size : {}",subList.size());
            stepExecution.setReadCount(toIndex);

//            return RepeatStatus.FINISHED;
            return RepeatStatus.CONTINUABLE; //이코드를 반복해라
        };
    }

    private List<String> getItems() {
        List<String> items = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            items.add(i + " Hello");
        }
        return items;
    }
}
