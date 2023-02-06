package com.niudong.esdemo.service.impl;

import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.stereotype.Service;

import com.niudong.esdemo.service.FieldCapabilitiesService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述跨索引的字段搜索相关API使用和测试
 *
 */
@Service
public class FieldCapabilitiesServiceImpl implements FieldCapabilitiesService {
  private static Log log = LogFactory.getLog(FieldCapabilitiesServiceImpl.class);

  private RestHighLevelClient restClient;

  // 初始化连接
  @PostConstruct
  public void initEs() {
    restClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));

    log.info("ElasticSearch init in service.");
  }

  // 关闭连接
  public void closeEs() {
    try {
      restClient.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // 构建FieldCapabilitiesRequest
  public FieldCapabilitiesRequest buildFieldCapabilitiesRequest() {
    FieldCapabilitiesRequest request =
        new FieldCapabilitiesRequest().fields("content").indices("ultraman", "ultraman1");

    // 配置可选参数indicesOptions：解析不可用索引以及展开通配符表达式
    request.indicesOptions(IndicesOptions.lenientExpandOpen());

    return request;
  }

  // 构建FieldCapabilitiesRequest
  public FieldCapabilitiesRequest buildFieldCapabilitiesRequest(String field, String[] indices) {
    FieldCapabilitiesRequest request =
        new FieldCapabilitiesRequest().fields(field).indices(indices[0], indices[1]);

    // 配置可选参数indicesOptions：解析不可用索引以及展开通配符表达式
    request.indicesOptions(IndicesOptions.lenientExpandOpen());

    return request;
  }

  // 同步方式执行FieldCapabilitiesRequest，跨索引字段搜索请求
  public void executeFieldCapabilitiesRequest(String field, String[] indices) {
    // 构建FieldCapabilitiesRequest
    FieldCapabilitiesRequest request = buildFieldCapabilitiesRequest(field, indices);

    try {
      FieldCapabilitiesResponse response = restClient.fieldCaps(request, RequestOptions.DEFAULT);

      // 处理返回结果FieldCapabilitiesResponse
      processFieldCapabilitiesResponse(response, field, indices);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 处理返回结果FieldCapabilitiesResponse
  private void processFieldCapabilitiesResponse(FieldCapabilitiesResponse response, String field,
      String[] indices) {
    // 获取字段中可能含有类型的映射
    Map<String, FieldCapabilities> fieldResponse = response.getField(field);
    Set<String> set = fieldResponse.keySet();
    // 获取文本字段类型下的数据
    FieldCapabilities textCapabilities = fieldResponse.get("text");

    // 数据能否被搜索到
    boolean isSearchable = textCapabilities.isSearchable();
    log.info("isSearchable is " + isSearchable);

    // 数据能否聚合
    boolean isAggregatable = textCapabilities.isAggregatable();
    log.info("isAggregatable is " + isAggregatable);

    // 获取特定字段类型下的索引
    String[] indicesArray = textCapabilities.indices();
    if (indicesArray != null) {
      log.info("indicesArray is " + indicesArray.length);
    }

    // field字段不能被搜索到的索引集合
    String[] nonSearchableIndices = textCapabilities.nonSearchableIndices();
    if (nonSearchableIndices != null) {
      log.info("nonSearchableIndices is " + nonSearchableIndices.length);
    }

    // field字段不能被聚合到的索引集合
    String[] nonAggregatableIndices = textCapabilities.nonAggregatableIndices();
    if (nonAggregatableIndices != null) {
      log.info("nonAggregatableIndices is " + nonAggregatableIndices.length);
    }
  }

  // 异步方式执行FieldCapabilitiesRequest，跨索引字段搜索请求
  public void executeFieldCapabilitiesRequestAsync(String field, String[] indices) {
    // 构建FieldCapabilitiesRequest
    FieldCapabilitiesRequest request = buildFieldCapabilitiesRequest(field, indices);

    // 配置监听器
    ActionListener<FieldCapabilitiesResponse> listener =
        new ActionListener<FieldCapabilitiesResponse>() {
          @Override
          public void onResponse(FieldCapabilitiesResponse response) {

        }

          @Override
          public void onFailure(Exception e) {

        }
        };

    // 执行异步请求
    try {
      restClient.fieldCapsAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }
}
