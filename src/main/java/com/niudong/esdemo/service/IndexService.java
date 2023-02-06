package com.niudong.esdemo.service;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述索引相关API使用和测试
 *
 */
public interface IndexService {
  // 同步执行AnalyzeRequest
  public void executeAnalyzeRequest(String text);

  // 同步执行创建索引的请求
  public void executeIndexRequest(String index);

  // 同步执行GetIndexRequest
  public void excuteGetIndexRequest(String index);

  // 同步执行DeleteIndexRequest
  public void executeDeleteIndexRequest(String index);

  // 同步执行索引存在验证请求
  public void executeExistsIndexRequest(String index);

  // 同步方式执行打开索引OpenIndexRequest
  public void executeOpenIndexRequest(String index);

  // 以同步方式执行关闭索引请求CloseIndexRequest
  public void executeCloseIndexRequest(String index);

  // 同步执行ResizeRequest
  public void executeResizeRequest(String sourceIndex, String targetIndex);

  // 同步执行拆分索引的ResizeRequest
  public void executeSplitRequest(String sourceIndex, String targetIndex);

  // 同步方式执行RefreshRequest
  public void executeRefreshRequest(String index);

  // 同步方式执行FlushRequest
  public void executeFlushRequest(String index);

  // 同步方式执行SyncedFlushRequest
  public void executeSyncedFlushRequest(String index);

  // 同步方式执行ClearIndicesCacheRequest
  public void executeClearIndicesCacheRequest(String index);

  // 同步执行ForceMergeRequest
  public void executeForceMergeRequest(String index);

  // 同步执行RolloverRequest
  public void executeRolloverRequest(String index);

  // 同步执行IndicatesAliasesRequest
  public void executeIndicatesAliasesRequest(String index, String indexAlias);

  // 同步执行GetAliasesRequest
  public void executeGetAliasesRequest(String indexAlias);

  // 同步执行GetAliasesRequest,获取索引别名
  public void executeGetAliasesRequestForAliases(String indexAlias);
}
