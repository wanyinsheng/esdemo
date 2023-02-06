package com.niudong.esdemo.controller;

import org.elasticsearch.common.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.niudong.esdemo.service.ScrollSearchService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述滚动搜索相关API使用和测试
 *
 */
@RestController
@RequestMapping("/springboot/es/scrollsearch")
public class ScrollSearchController {
  @Autowired
  private ScrollSearchService scrollSearchService;

  // 同步方式执行SearchScrollRequest
  @RequestMapping("/sr")
  public String executeSearchRequest(String indexName, int size) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName) || size <= 0) {
      return "Parameters are wrong!";
    }

    scrollSearchService.buildAndExecuteScrollSearchRequest(indexName, size);
    return "Execute SearchScrollRequest success!";
  }
}
