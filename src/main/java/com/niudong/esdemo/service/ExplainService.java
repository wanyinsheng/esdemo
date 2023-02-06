package com.niudong.esdemo.service;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述搜索解释相关API使用和测试
 *
 */
public interface ExplainService {
  // 同步方式执行ExplainRequest
  public void executeExplainRequest(String indexName, String document, String field,
      String content);
}
