package com.niudong.esdemo.controller;

import org.elasticsearch.common.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.niudong.esdemo.service.RankEvalService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述跨索引字段搜索相关API使用和测试
 *
 */
@RestController
@RequestMapping("/springboot/es/ranksearch")
public class RankEvalController {
  @Autowired
  private RankEvalService rankEvalService;

  // 同步方式执行MultiSearchRequest
  @RequestMapping("/sr")
  public String executeRankEvalRequest(String indexName, String document, String field,
      String content) {
    // 参数校验
    if (Strings.isNullOrEmpty(indexName) || Strings.isNullOrEmpty(document)
        || Strings.isNullOrEmpty(field) || Strings.isNullOrEmpty(content)) {
      return "Parameters are wrong!";
    }

    rankEvalService.executeRankEvalRequest(indexName, document, field, content);

    return "Execute RankEvalRequest success!";
  }
}
