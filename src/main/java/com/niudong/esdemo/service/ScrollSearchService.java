package com.niudong.esdemo.service;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述滚动搜索相关API使用和测试
 *
 */
public interface ScrollSearchService {
  // 构建SearchRequest:带有滚动搜索参数
  public void buildAndExecuteScrollSearchRequest(String indexName, int size);
}
