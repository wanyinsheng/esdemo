package com.niudong.esdemo.service;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述高级ES客户端的使用和测试
 *
 */
public interface MeetHighElasticSearchService {
  /**
   * 本部分用于介绍如何与ElasticSearch构建连接和关闭连接
   */
  public void initEs();

  public void closeEs();

  // 索引文档
  public void indexDocuments(String indexName, String document);

  // 同步方式获取索引文档
  public void getIndexDocuments(String indexName, String document);

  // 同步方式校验索引文档是否存在
  public void checkExistIndexDocuments(String indexName, String document);

  // 同步方式删除索引文档
  public void deleteIndexDocuments(String indexName, String document);

  // 同步方式更新索引文档
  public void updateIndexDocuments(String indexName, String document);

  // 同步执行TermVectorsRequest请求
  public void exucateTermVectorsRequest(String indexName, String document, String field);

  // 同步方式执行BulkRequest
  public void executeBulkRequest(String indexName, String field);

  // 用BulkProcessor在同步方式下 执行BulkRequest
  public void executeBulkRequestWithBulkProcessor(String indexName, String field);

  // 同步执行MultiGetRequest
  public void executeMultiGetRequest(String indexName, String[] documentIds);

  // 同步执行ReindexRequest
  public void executeReindexRequest(String fromIndex, String toIndex);

  // 同步执行UpdateByQueryRequest
  public void executeUpdateByQueryRequest(String indexName);

  // 同步执行 DeleteByQueryRequest
  public void executeDeleteByQueryRequest(String indexName);

  // 同步执行MultiTermVectorsRequest
  public void executeMultiTermVectorsRequest(String indexName, String[] documentIds, String field);
}
