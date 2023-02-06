package com.niudong.esdemo.service;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述跨索引的字段搜索相关API使用和测试
 *
 */
public interface FieldCapabilitiesService {
  // 同步方式执行FieldCapabilitiesRequest，跨索引字段搜索请求
  public void executeFieldCapabilitiesRequest(String field, String[] indices);
}
