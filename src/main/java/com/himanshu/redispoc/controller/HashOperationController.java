package com.himanshu.redispoc.controller;

import com.himanshu.redispoc.entity.Employee;
import com.himanshu.redispoc.entity.IndexSearch;
import com.himanshu.redispoc.service.HashOperationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("hash")
@Slf4j
public class HashOperationController {
    HashOperationService hashOperationService;
    public HashOperationController(HashOperationService hashOperationService){
        this.hashOperationService = hashOperationService;
    }

    /*
    Create employee in redis
     */
    @PostMapping("employee")
    public ResponseEntity<Boolean> createEmployee(@RequestBody Employee employee){
        log.info("Received request to create employee " + employee);
        return new ResponseEntity<>(hashOperationService.saveEmployeeToRedisHash(employee), HttpStatus.CREATED);
    }

    /*
    Get employee by id from redis
     */
    @GetMapping("employee/{id}")
    public ResponseEntity<Employee> getEmployee(@PathVariable String id){
        log.info("Received request to fetch employee " + id);
        return new ResponseEntity<>(hashOperationService.getEmployeeFromRedisHash(id), HttpStatus.FOUND);
    }

    /*
    Search employee in idx:employee index
     */
    @PostMapping("employee/search")
    public ResponseEntity<List<Employee>> searchEmployee(@RequestBody IndexSearch search){
        log.info("Received request to search employee " + search);
        return new ResponseEntity<>(hashOperationService.searchEmployeeFromRedisHash(search), HttpStatus.OK);
    }

    /*
    Delete an employee
     */
    @DeleteMapping("employee/{id}")
    public ResponseEntity<Boolean> deleteEmployee(@PathVariable String id){
        log.info("Received request to delete employee " + id);
        return new ResponseEntity<>(hashOperationService.deleteEmployeeHashFromRedis(id),HttpStatus.OK);
    }


}
