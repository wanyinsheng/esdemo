package com.niudong.esdemo.controller;

import java.util.List;

import org.elasticsearch.common.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.base.Splitter;
import com.niudong.esdemo.service.MeetHighElasticSearchService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述高级ES客户端的使用和测试
 *
 */
@RestController
@RequestMapping("/springboot/es/high")
public class MeetHighElasticSearchController {
  @Autowired
  private MeetHighElasticSearchService meetHighElasticSearchService;

  // 索引文档
  @RequestMapping("/index/put")
  public String putIndexInHighElasticSearch(String indexName, String document) {
    if (Strings.isEmpty(indexName) || Strings.isEmpty(document)) {
      return "Parameters are error!";
    }
    meetHighElasticSearchService.indexDocuments(indexName, document);
    return "Index High ElasticSearch Client Successed!";
  }

  // 同步方式获取索引文档
  @RequestMapping("/index/get")
  public String getIndexInHighElasticSearch(String indexName, String document) {
    if (Strings.isEmpty(indexName) || Strings.isEmpty(document)) {
      return "Parameters are error!";
    }
    meetHighElasticSearchService.getIndexDocuments(indexName, document);
    return "Get Index High ElasticSearch Client Successed!";
  }

  // 同步方式校验索引文档是否存在
  @RequestMapping("/index/check")
  public String checkIndexInHighElasticSearch(String indexName, String document) {
    if (Strings.isEmpty(indexName) || Strings.isEmpty(document)) {
      return "Parameters are error!";
    }
    meetHighElasticSearchService.checkExistIndexDocuments(indexName, document);
    return "Check Index High ElasticSearch Client Successed!";
  }

  // 同步方式删除索引文档是否存在
  @RequestMapping("/index/delete")
  public String deleteIndexInHighElasticSearch(String indexName, String document) {
    if (Strings.isEmpty(indexName) || Strings.isEmpty(document)) {
      return "Parameters are error!";
    }
    meetHighElasticSearchService.deleteIndexDocuments(indexName, document);
    return "Delete Index High ElasticSearch Client Successed!";
  }

  // 同步方式更新索引文档是否存在
  @RequestMapping("/index/update")
  public String updateIndexInHighElasticSearch(String indexName, String document) {
    if (Strings.isEmpty(indexName) || Strings.isEmpty(document)) {
      return "Parameters are error!";
    }
    meetHighElasticSearchService.updateIndexDocuments(indexName, document);
    return "Update Index High ElasticSearch Client Successed!";
  }

  // 同步执行TermVectorsRequest请求
  @RequestMapping("/index/term")
  public String termVectorsInHighElasticSearch(String indexName, String document, String field) {
    if (Strings.isEmpty(indexName) || Strings.isEmpty(document) || Strings.isEmpty(field)) {
      return "Parameters are error!";
    }
    meetHighElasticSearchService.exucateTermVectorsRequest(indexName, document, field);
    return "Test TermVectorsRequest High ElasticSearch Client Successed!";
  }

  // 同步执行BulkRequest请求
  @RequestMapping("/index/bulk")
  public String bulkGetInHighElasticSearch(String indexName, String field) {
    if (Strings.isEmpty(indexName) || Strings.isEmpty(field)) {
      return "Parameters are error!";
    }
    meetHighElasticSearchService.executeBulkRequest(indexName, field);
    return "Bulk Get In High ElasticSearch Client Successed!";
  }

  // 基于BulkProcessor同步执行BulkRequest请求
  @RequestMapping("/index/bulkProcessor")
  public String bulkProcessorGetInHighElasticSearch(String indexName, String field) {
    if (Strings.isEmpty(indexName) || Strings.isEmpty(field)) {
      return "Parameters are error!";
    }
    meetHighElasticSearchService.executeBulkRequestWithBulkProcessor(indexName, field);
    return "BulkProcessor Get In High ElasticSearch Client Successed!";
  }

  // 同步执行MultiGet请求
  @RequestMapping("/index/multiget")
  public String multigetInHighElasticSearch(String indexName, String documentId) {
    if (Strings.isEmpty(indexName) || Strings.isEmpty(documentId)) {
      return "Parameters are error!";
    }

    // 将field(英文逗号分隔的)转化成String[]
    List<String> documentIds = Splitter.on(",").splitToList(documentId);

    meetHighElasticSearchService.executeMultiGetRequest(indexName,
        documentIds.toArray(new String[documentIds.size()]));
    return "MultiGet In High ElasticSearch Client Successed!";
  }

  // 同步执行 reindex 请求
  @RequestMapping("/index/reindex")
  public String reindexInHighElasticSearch(String fromIndexName, String toIndexName) {
    if (Strings.isEmpty(fromIndexName) || Strings.isEmpty(toIndexName)) {
      return "Parameters are error!";
    }

    meetHighElasticSearchService.executeReindexRequest(fromIndexName, toIndexName);
    return "Reindex In High ElasticSearch Client Successed!";
  }

  // 同步执行 updateByQuery 请求
  @RequestMapping("/index/updateByQuery")
  public String updateByQueryInHighElasticSearch(String indexName) {
    if (Strings.isEmpty(indexName)) {
      return "Parameters are error!";
    }

    meetHighElasticSearchService.executeUpdateByQueryRequest(indexName);
    return "UpdateByQuery In High ElasticSearch Client Successed!";
  }

  // 同步执行 DeleteByQuery 请求
  @RequestMapping("/index/deleteByQuery")
  public String deleteByQueryInHighElasticSearch(String indexName) {
    if (Strings.isEmpty(indexName)) {
      return "Parameters are error!";
    }

    meetHighElasticSearchService.executeDeleteByQueryRequest(indexName);
    return "DeleteByQuery In High ElasticSearch Client Successed!";
  }

  // 同步执行 MultiTermVectorsRequest 请求
  @RequestMapping("/index/multiterm")
  public String multitermInHighElasticSearch(String indexName, String documentId, String field) {
    if (Strings.isEmpty(indexName) || Strings.isEmpty(documentId) || Strings.isEmpty(field)) {
      return "Parameters are error!";
    }

    // 将field(英文逗号分隔的)转化成String[]
    List<String> documentIds = Splitter.on(",").splitToList(documentId);

    meetHighElasticSearchService.executeMultiTermVectorsRequest(indexName,
        documentIds.toArray(new String[documentIds.size()]), field);
    return "MultiTermVectorsRequest In High ElasticSearch Client Successed!";
  }
}
