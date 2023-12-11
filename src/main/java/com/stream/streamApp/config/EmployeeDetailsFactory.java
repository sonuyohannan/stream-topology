package com.stream.streamApp.config;

import com.kafka.producerapp.EmployeeAdressDetails;
import com.kafka.producerapp.EmployeeDetails;
import com.kafka.producerapp.EmployeePersonalDetails;

public class EmployeeDetailsFactory {
    public static EmployeeDetails setEmployeeDetails(EmployeePersonalDetails employeePersonaldetail, EmployeeAdressDetails employeeAddressDetail){
        EmployeeDetails employeeDetal= new EmployeeDetails();
        if(employeePersonaldetail!=null){
            employeeDetal.setEmployeeId(employeePersonaldetail.getEmployeeId());
            employeeDetal.setFirstname(employeePersonaldetail.getFirstname());
            employeeDetal.setLastname(employeePersonaldetail.getLastname());
            employeeDetal.setAge(employeePersonaldetail.getAge());
            employeeDetal.setSex(employeePersonaldetail.getSex());

        }

        if(employeeAddressDetail!=null){
            employeeDetal.setEmployeeId(employeeAddressDetail.getEmployeeId());
            employeeDetal.setHousename(employeeAddressDetail.getHousename());
            employeeDetal.setCity(employeeAddressDetail.getCity());
            employeeDetal.setCountry(employeeAddressDetail.getCountry());
            employeeDetal.setPostcode(employeeAddressDetail.getPostcode());
            employeeDetal.setState(employeeAddressDetail.getState());


        }
        return employeeDetal;
    }

    public static EmployeeDetails aggregateSet(EmployeeDetails oldValue, EmployeeDetails newValue) {
        oldValue.setEmployeeId(newValue.getEmployeeId());
        oldValue.setFirstname(newValue.getFirstname());
        oldValue.setLastname(newValue.getLastname());
        oldValue.setAge(newValue.getAge());
        oldValue.setSex(newValue.getSex());
        oldValue.setHousename(newValue.getHousename());
        oldValue.setStreetname(newValue.getStreetname());
        oldValue.setCity(newValue.getCity());
        oldValue.setPostcode(newValue.getPostcode());
        oldValue.setDistrict(newValue.getDistrict());
        oldValue.setState(newValue.getState());
        oldValue.setCountry(newValue.getCountry());
        return oldValue;
    }
}
