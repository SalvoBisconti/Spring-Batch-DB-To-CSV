package com.advancia.stage.start;


import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import com.advancia.stage.model.User;
import com.zaxxer.hikari.HikariDataSource;



@Configuration
public class BatchConfiguration {

    @Bean
    public DataSource dataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setJdbcUrl("jdbc:mysql://localhost:3306/SpringEx");
        dataSource.setUsername("root");
        dataSource.setPassword("bisconti98");
        return dataSource;
    }

    @Bean
    public JdbcCursorItemReader<User> reader(DataSource dataSource) {
        JdbcCursorItemReader<User> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("SELECT id, name FROM user");
        reader.setRowMapper(new UserRowMapper());
        return reader;
    }

    public class UserRowMapper implements RowMapper<User> {
        @Override
        public User mapRow(ResultSet rs, int rowNum) throws SQLException {
            User user = new User();
            user.setId(rs.getInt("id"));
            user.setName(rs.getString("name"));
            return user;
        }
    }

    @Bean
    public UserItemProcessor processor() {
        return new UserItemProcessor();
    }

    @Bean
    public FlatFileItemWriter<User> writer() {
        FlatFileItemWriter<User> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("users.csv"));
        writer.setLineAggregator(new DelimitedLineAggregator<User>() {{
            setDelimiter(",");
            setFieldExtractor(new BeanWrapperFieldExtractor<User>() {{
                setNames(new String[]{"id", "name"});
            }});
        }});
        return writer;
    }

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                      JdbcCursorItemReader<User> reader, UserItemProcessor processor, FlatFileItemWriter<User> writer) {
        return new StepBuilder("step1", jobRepository)
                .<User, User>chunk(10, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public Job exportUserJob(JobRepository jobRepository, Step step1) {
        return new JobBuilder("exportUserJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .flow(step1)
                .end()
                .build();
    }
}
