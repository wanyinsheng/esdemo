package com.niudong.esdemo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.niudong.esdemo.service.SearchService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述搜索相关API使用和测试
 *
 */
@RestController
@RequestMapping("/springboot/es/search")
public class SearchController {
  @Autowired
  private SearchService searchService;
  
  // 同步方式执行SearchRequest
  @RequestMapping("/sr")
  public String executeSearchRequest() {
    searchService.executeSearchRequest();
    
    return "Execute SearchRequest success!";
  }
  
  
}
