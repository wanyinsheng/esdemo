package com.niudong.esdemo.service;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述搜索结果评估相关API使用和测试
 *
 */
public interface RankEvalService {
  // 同步执行RankEvalRequest
  public void executeRankEvalRequest(String index, String documentId, String field, String content);
}
