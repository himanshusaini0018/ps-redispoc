package com.himanshu.redispoc.dto;

import com.himanshu.redispoc.entity.Employee;
import com.himanshu.redispoc.entity.IndexSearch;
import com.himanshu.redispoc.exception.AlreadyExistException;
import com.himanshu.redispoc.util.RedispocConstantUtils;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.search.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class HashOperationDto {

    RedisTemplate<String, Object> redisTemplate;
    JedisPooled jedis;

    public HashOperationDto(RedisTemplate<String,Object> redisTemplate,JedisPooled jedis) {
        this.redisTemplate = redisTemplate;
        this.jedis = jedis;
    }
    /*
    Creating index for employee,
    Index Name : idx:employee
     */
    @PostConstruct
    private void createIndex(){
        log.info("Creating index for employee in redis");
        redisTemplate.execute((RedisConnection connection) -> {
            try {
                /*
                Added schema fields. name, department and salary
                 */
                Schema schema = new Schema()
                        .addTextField("name",1)
                        .addTagField("department")
                        .addNumericField("salary");
                log.debug("Using schema {}",schema.fields);
                IndexDefinition indexDefinition = new IndexDefinition(IndexDefinition.Type.HASH)
                        .setPrefixes("employee:");
                log.debug("Using indexDefinition {}",indexDefinition);
                IndexOptions indexOptions = IndexOptions.defaultOptions().setDefinition(indexDefinition);
                jedis.ftCreate("idx:employee",indexOptions,schema);
                log.info("Created index for employee in redis");
                return null;
            } catch (Exception e) {
                log.error(e.getMessage(),e);
            }
            return null;
        });
    }


    /*
    Saving employee to redis instance
     */
    public boolean savetoHash(Employee value){
        log.info("Entered savetoHash");
        log.info("Saving employee with id {}",value.getId());
        //Constructed key for storage in redis
        String constructedHash = RedispocConstantUtils.createHash(value.getId().toString());
        //Using session-callback for transactional operations
        return redisTemplate.execute(new SessionCallback<Boolean>() {
            @Override
            public <K,V> Boolean execute(RedisOperations<K, V> operations) throws DataAccessException {
                RedisOperations<String,Object> ops = (RedisOperations<String,Object>) operations;
                int attempt = 0;
                int maxAttempts = 3;
                //Retrying in case of transaction skipped due to some concurrent issues
                while (attempt < maxAttempts) {
                    //Creating watch for constructedHash in redis db
                    ops.watch(constructedHash);
                    log.debug("Attempting to watch hash {}",constructedHash);
                    //return if hash is already present in database
                    if (Boolean.TRUE.equals(ops.hasKey(constructedHash))) {
                        ops.unwatch();
                        log.error("Employee hash {} already exists",constructedHash);
                        throw new AlreadyExistException("Employee id already exists");
                    }
                    try {
                        ops.multi();
                        //creating map of employee values
                        Map<String, Object> map = Map.of(
                                "name", value.getName(),
                                "department", value.getDepartment(),
                                "salary", value.getSalary()
                        );
                        //making pipeline of command to be executing
                        ops.opsForHash().putAll(constructedHash, map);
                        ops.hasKey(constructedHash);
                        log.debug("Executing insert operations for {}",constructedHash);
                        //executing all command at once
                        List<Object> execResult = ops.exec();
                        if (!execResult.isEmpty()) {
                            log.info("Successfully inserted employee with id {}",constructedHash);
                            return true;
                        }
                        //retrying
                        attempt++;
                        log.info("Insert operation failed for {}, retrying attempt {}",constructedHash,attempt);
                    } catch (Exception e) {
                        ops.discard();
                        throw new RuntimeException(e);
                    }
                }
                log.error("Failed to insert employee with id {}, retried for {} times",constructedHash,attempt);
                return false;
            }
        });
    }

    public Employee getEmployeeFromHash(String id){
        log.info("Entered getEmployeeFromHash");
        //creating key for employee object
        String constructedHash = RedispocConstantUtils.createHash(id);
        log.info("Getting employee with id {}",constructedHash);
        //fetching employee details from redis
        Map<Object, Object> employeeEntries = redisTemplate.opsForHash().entries(constructedHash);
        //deserializing employee object before sending
        return RedispocConstantUtils.deserializeEmployee(employeeEntries);
    }

    public List<Employee> searchEmployeeFromHash(IndexSearch indexSearch){
        log.info("Entered searchEmployeeFromHash");
        List<Employee> employees = new ArrayList<>();
        log.info("Fetching employees with query as {}",indexSearch);
        //Executing a index search result using jedisPooled
        SearchResult searchResult = jedis.ftSearch("idx:employee",indexSearch.getSearchKey());
        log.debug("Employee list: {}",searchResult);
        for(Document doc:searchResult.getDocuments()){
            //deserializing employees one by one
            Employee employee = RedispocConstantUtils.deserializeEmployeeHashSearchResults(doc);
            employees.add(employee);
        }
        log.info("Searching for employee returned {}",employees);
        return employees;
    }


    public Boolean deleteEmployeeById(String id) {
        log.info("Entered deleteEmployeeById");
        //creating a hash for employee to be deleted
        log.info("Deleting employee with id {}",id);
        String createHash = RedispocConstantUtils.createHash(id);
        return  redisTemplate.execute(new SessionCallback<Boolean>() {
            @Override
            public <K,V> Boolean execute(RedisOperations<K, V> operations) throws DataAccessException {
                RedisOperations<String,Object> ops = (RedisOperations<String,Object>) operations;
                int attempt = 0;
                int maxAttempts = 3;
                //Retrying for 3 times
                while (attempt < maxAttempts) {
                    //Watching for hash in redis
                    ops.watch(createHash);
                    //Return in case of id with hash not found in redis
                    if (Boolean.FALSE.equals(ops.hasKey(createHash))) {
                        log.debug("Employee with id {} do not exists",createHash);
                        ops.unwatch();
                        throw new RuntimeException("Employee with id " + id + " not found");
                    }
                    try {
                        ops.multi();
                        ops.delete(createHash);
                        //execute delete operation
                        List<Object> execResult = ops.exec();
                        //return true if execResult retuned a response
                        log.debug("Executing delete operation for {} returned {}",createHash,execResult);
                        if(!execResult.isEmpty()){
                            return true;
                        }
                        //increase retry counter
                        attempt++;
                        log.info("Retrying delete operation for {} {} times",createHash,attempt);
                    }catch (RuntimeException e){
                        ops.discard();
                        throw new RuntimeException(e);
                    }
                }
                //return false if not successfully deleted after 3 try
                return false;
            }
        });
    }
}
