package com.himanshu.redispoc.service;

import com.himanshu.redispoc.dto.HashOperationDto;
import com.himanshu.redispoc.entity.Employee;
import com.himanshu.redispoc.entity.IndexSearch;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class HashOperationService {
    HashOperationDto hashOperationDto;

    public HashOperationService(HashOperationDto hashOperationDto){

        this.hashOperationDto = hashOperationDto;
    }


    public boolean saveEmployeeToRedisHash(Employee employee){
        log.info("Entered saveEmployeeToRedisHash");
        return hashOperationDto.savetoHash(employee);
    }

    public Employee getEmployeeFromRedisHash(String id){
        log.info("Entered getEmployeeFromRedisHash");
        Employee employee = hashOperationDto.getEmployeeFromHash(id);
        if(employee != null){
            Long empId = Long.parseLong(id);
            employee.setId(empId);
        }else {
            log.error("Employee not found with id " + id);
            throw new RuntimeException("Employee not found with id: "+id);
        }
        return employee;
    }

    public List<Employee> searchEmployeeFromRedisHash(IndexSearch indexSearch){
        log.info("Entered searchEmployeeFromRedisHash");
        return hashOperationDto.searchEmployeeFromHash(indexSearch);
    }

    public Boolean deleteEmployeeHashFromRedis(String id) {
        log.info("Entered deleteEmployeeHashFromRedis");
        return hashOperationDto.deleteEmployeeById(id);
    }
}
