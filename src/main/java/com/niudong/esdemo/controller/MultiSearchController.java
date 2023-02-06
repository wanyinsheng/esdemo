package com.niudong.esdemo.controller;

import java.util.List;

import org.elasticsearch.common.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.base.Splitter;
import com.niudong.esdemo.service.MultiSearchService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述批量搜索相关API使用和测试
 *
 */
@RestController
@RequestMapping("/springboot/es/multisearch")
public class MultiSearchController {
  @Autowired
  private MultiSearchService multiSearchService;

  // 同步方式执行MultiSearchRequest
  @RequestMapping("/sr")
  public String executeMultiSearchRequest(String field, String keywords) {
    // 参数校验
    if (Strings.isNullOrEmpty(field) || Strings.isNullOrEmpty(keywords)) {
      return "Parameters are wrong!";
    }

    // 将英文逗号分隔的字符串切分成数组
    List<String> keywordsList = Splitter.on(",").splitToList(keywords);
    multiSearchService.executeMultiSearchRequest(field,
        keywordsList.toArray(new String[keywordsList.size()]));
    return "Execute MultiSearchRequest success!";
  }
}
