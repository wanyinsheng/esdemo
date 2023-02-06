package com.niudong.esdemo.service;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述批量搜索相关API使用和测试
 *
 */
public interface MultiSearchService {
  // 同步执行MultiSearchRequest
  public void executeMultiSearchRequest(String field, String[] keywords);
}
