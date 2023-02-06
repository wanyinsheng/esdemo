package com.niudong.esdemo.controller;

import org.elasticsearch.common.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.niudong.esdemo.service.ExplainService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述搜索解释相关API使用和测试
 *
 */
@RestController
@RequestMapping("/springboot/es/explainsearch")
public class ExplainController {
  @Autowired
  private ExplainService explainService;

  // 同步方式执行ExplainRequest
  @RequestMapping("/sr")
  public String executeExplainRequest(String indexName, String document, String field,
      String content) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName) || Strings.isNullOrEmpty(document)
        || Strings.isNullOrEmpty(field) || Strings.isNullOrEmpty(content)) {
      return "Parameters are wrong!";
    }

    explainService.executeExplainRequest(indexName, document, field, content);

    return "Execute ExplainRequest success!";
  }
}
