package com.niudong.esdemo.service;

import org.elasticsearch.client.Request;

/**
 * 
 * @author 牛冬
 * @desc:ES低级客户端初始化相关操作
 *
 */
public interface MeetElasticSearchService {
  /**
   * 本部分用于介绍如何与ElasticSearch构建连接和关闭连接
   */
  public void initEs();

  public void closeEs();

  /**
   * 本部分用于介绍如何构建对ElasticSearch服务的请求
   */
  public String executeRequest();

  /**
   * 本部分用于介绍如何解析ElasticSearch服务的返回结果
   */
  public void parseElasticSearchResponse();
}
