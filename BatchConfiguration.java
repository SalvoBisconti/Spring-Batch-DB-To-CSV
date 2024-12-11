package com.advancia.stage.start;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;

import com.advancia.stage.model.User;


@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	

	 @Autowired
	 public DataSource dataSource; 
	 
	 @Autowired
	 private PlatformTransactionManager transactionManager;
	 
	 @Bean
	 public DataSource dataSource() {
		    DriverManagerDataSource dataSource = new DriverManagerDataSource();

		  dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
		  dataSource.setUrl("jdbc:mysql://localhost:3306/SpringEx");
		  dataSource.setUsername("root");
		  dataSource.setPassword("bisconti98");
		  
		    return dataSource;
		}
	 

	 @Bean
	 public JdbcCursorItemReader<User> reader() throws Exception {
	     JdbcCursorItemReader<User> reader = new JdbcCursorItemReader<>();
	     reader.setDataSource(dataSource);
	     reader.setSql("SELECT id, name FROM user");
	     reader.setRowMapper(new UserRowMapper());
	     
	     reader.open(new ExecutionContext());

	     /*
	     try (Connection conn = dataSource.getConnection();
	          PreparedStatement stmt = conn.prepareStatement("SELECT id, name FROM user");
	          ResultSet rs = stmt.executeQuery()) {
	         if (rs.next()) {
	             System.err.println("Query eseguita con successo: " + rs.getInt("id") + ", " + rs.getString("name"));
	         } else {
	             System.err.println("La query non ha restituito risultati!");
	         }
	     } catch (SQLException e) {
	         System.err.println("Errore durante l'esecuzione della query manuale: " + e.getMessage());
	     }
*/
		    User user;
		    while ((user = reader.read()) != null) {
		        System.err.println("Dati letti dal database: " + user);
		    }

		    reader.close();
	     
	     return reader;
	 }

	 
	 
	 
	 public class UserRowMapper implements RowMapper<User>{

	  @Override
	  public User mapRow(ResultSet rs, int rowNum) throws SQLException {
		  System.err.println("Row mappata: ID=" + rs.getInt("id") + ", Name=" + rs.getString("name"));
	   User user = new User();
	   user.setId(rs.getInt("id"));
	   user.setName(rs.getString("name"));
   
	   return user;
	  }
	  
	 }
	 
	 @Bean
	 public UserItemProcessor processor(){
	  return new UserItemProcessor();
	 }
	 
	 
	 @Bean
	 public FlatFileItemWriter<User> writer(){
	  FlatFileItemWriter<User> writer = new FlatFileItemWriter<>();
	  writer.setResource(new FileSystemResource("users.csv"));
	  writer.setLineAggregator(new DelimitedLineAggregator<User>() {{
	   setDelimiter(",");
	   setFieldExtractor(new BeanWrapperFieldExtractor<User>() {{
	    setNames(new String[] { "id", "name" });
	   }});
	   
	  }});
	  /*
	  
	    List<User> testUsers = Arrays.asList(new User(1, "Jack"), new User(2, "John"));
	    Chunk<User> chunk = new Chunk<>(testUsers);

	    try {
	        writer.open(new ExecutionContext());
	        writer.write(chunk);  
	    } catch (Exception e) {
	        System.err.println("Errore durante la scrittura: " + e.getMessage());
	    }

*/

	    System.err.println("Configurazione del writer completata.");
	  return writer;
	 }
	 
	 
	    @Bean
	    public CompletionPolicy completionPolicy() {
	        return new SimpleCompletionPolicy(10);
	    }
	    
	    /*
	    @Bean
	    public ItemReadListener<User> itemReadListener() {
	        return new ItemReadListener<User>() {
	            @Override
	            public void beforeRead() {
	                System.out.println("Prima di leggere l'item...");
	            }

	            @Override
	            public void afterRead(User item) {
	                System.out.println("Dopo aver letto l'item: " + item);
	            }

	            @Override
	            public void onReadError(Exception ex) {
	                System.err.println("Errore durante la lettura: " + ex.getMessage());
	            }
	        };
	    }


	    
	
	    
	    @Bean
	    public ItemWriteListener<User> itemWriteListener() {
	        return new ItemWriteListener<User>() {

	            public void beforeWrite(List<? extends User> items) {
	                System.out.println("Prima di scrivere gli item: " + items);
	            }


	            public void afterWrite(List<? extends User> items) {
	                System.out.println("Dopo aver scritto gli item: " + items);
	                System.out.println("Contenuto scritto: ");
	                items.forEach(item -> System.out.println("User ID: " + item.getId() + ", Name: " + item.getName()));
	            }


	            public void onWriteError(Exception exception, List<? extends User> items) {
	                System.err.println("Errore durante la scrittura: " + exception.getMessage());
	            }
	        };
	    }

	    */

	 
	    @Bean
	    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws Exception {
	    	System.err.println("Invocazione dello step1 e del reader()");
	        return new StepBuilder("step1", jobRepository)
	        		.<User, User>chunk(completionPolicy(), transactionManager)
	                .reader(reader())
	                
	                .processor(processor())
	                .writer(writer()) 
	            //    .listener(itemReadListener()) 
	            //   .listener(itemWriteListener()) 
	                .build();
	    }
	    
	    
	    /*
	    @Bean
	    public JobExecutionListener jobExecutionListener() {
	        return new JobExecutionListener() {
	            @Override
	            public void beforeJob(JobExecution jobExecution) {
	                System.out.println("Inizio del job.");
	            }

	            @Override
	            public void afterJob(JobExecution jobExecution) {
	                System.out.println("Fine del job. Stato: " + jobExecution.getStatus());
	            }
	        };
	    }
	    */

	    @Bean
	    public Job exportUserJob(JobRepository jobRepository) throws Exception {
	    	
	    	System.err.println("Configurazione del job");
	        return new JobBuilder("exportUserJob", jobRepository)
	                .incrementer(new RunIdIncrementer())
	                .start(step1(jobRepository, transactionManager ))
	              //  .listener(jobExecutionListener()) 
	                .build(); 
	    }
	     
	}