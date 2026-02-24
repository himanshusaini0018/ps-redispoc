package com.himanshu.redispoc.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.himanshu.redispoc.entity.Employee;
import io.redisearch.Document;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
@Slf4j
public class RedispocConstantUtils {
    private final static ObjectMapper mapper = new ObjectMapper();
    public static String createHash(String id){
        return  "employee:" + id;
    }


    public static Employee deserializeEmployee(Map<Object, Object> valueMap){
        log.debug("deserialize employee map {}",valueMap);
        if(valueMap == null||valueMap.isEmpty()) return null;
        return mapper.convertValue(valueMap, Employee.class);
    }



    public static Employee deserializeEmployeeHashSearchResults(Document doc){
        log.debug("deserialize employee hash search results {}",doc);
        Employee emp = new Employee();
        String redisKey = doc.getId();
        String rawId = redisKey.replace("employee:", "");
        Long id = Long.parseLong(rawId);
        emp.setId(id);
        if (doc.hasProperty("name")) {
            emp.setName(doc.getString("name"));
        }

        if (doc.hasProperty("department")) {
            emp.setDepartment(doc.getString("department"));
        }

        if (doc.hasProperty("salary")) {
            emp.setSalary(Double.parseDouble(doc.getString("salary")));
        }
        log.debug("deserialized employee: {}",emp);
        return emp;
    }



}
