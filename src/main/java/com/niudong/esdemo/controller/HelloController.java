package com.niudong.esdemo.controller;


import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/springboot/es")
public class HelloController {
  
  @RequestMapping("/test")
  public String index(int index) {
      return "Greetings from Spring Boot 2.1.6  for elasticsearch!"+index;
  }
}
