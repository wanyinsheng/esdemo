package com.niudong.esdemo.service.impl;

import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.stereotype.Service;

import com.niudong.esdemo.service.ExplainService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述搜索解释相关API使用和测试
 *
 */
@Service
public class ExplainServiceImpl implements ExplainService {
  private static Log log = LogFactory.getLog(ExplainServiceImpl.class);

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

  // 构建ExplainRequest
  public ExplainRequest buildExplainRequest(String indexName, String document, String field,
      String content) {
    ExplainRequest request = new ExplainRequest(indexName, document);
    request.query(QueryBuilders.termQuery(field, content));

    // 设置路由
    // request.routing("routing");

    // 使用首选参数，例如执行搜索以首选本地碎片。默认值是在分片之间随机进行。
    // request.preference("_local");

    // 设置为“真”以检索解释的文档源。您还可以通过使用“包含源代码”和“排除源代码”来检索部分文档
    // request.fetchSourceContext(new FetchSourceContext(true, new String[] {field}, null));

    // 允许控制一部分的存储字段（要求在映射中单独存储该字段）,返回作为说明文档
    // request.storedFields(new String[] {field});

    return request;
  }

  // 同步方式执行ExplainRequest
  public void executeExplainRequest(String indexName, String document, String field,
      String content) {
    // 构建ExplainRequest请求
    ExplainRequest request = buildExplainRequest(indexName, document, field, content);

    // 执行请求，接收返回结果
    try {
      ExplainResponse response = restClient.explain(request, RequestOptions.DEFAULT);

      // 解析ExplainResponse
      processExplainResponse(response);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }

  // 解析ExplainResponse
  private void processExplainResponse(ExplainResponse response) {
    // 解释文档的索引名称
    String index = response.getIndex();

    // 解释文档的ID
    String id = response.getId();

    // 解释的文档是否存在
    boolean exists = response.isExists();
    log.info("index is " + index + ";id is " + id + ";exists is " + exists);

    // 解释的文档与提供的查询之间是否匹配（匹配是从后台的Lucene解释中检索的，如果Lucene解释建模匹配，则返回true，否则返回false）。
    boolean match = response.isMatch();

    // 是否存在此请求的Lucene解释。
    boolean hasExplanation = response.hasExplanation();
    log.info("match is " + match + ";hasExplanation is " + hasExplanation);

    // 获取Lucene解释对象（如果存在）。
    Explanation explanation = response.getExplanation();
    if (explanation != null) {
      log.info("explanation is " + explanation.toString());
    }

    // 如果检索到源或存储字段，则获取getresult对象。
    GetResult getResult = response.getGetResult();
    if (getResult == null) {
      return;
    }

    // getresult在内部包含两个映射，用于存储提取的源字段和存储的字段。
    // 以Map的形式检索源。
    Map<String, Object> source = getResult.getSource();
    if (source == null) {
      return;
    }
    for (String str : source.keySet()) {
      log.info("str key is " + str);
    }

    // 以映射的形式检索指定的存储字段。
    Map<String, DocumentField> fields = getResult.getFields();
    if (fields == null) {
      return;
    }
    for (String str : fields.keySet()) {
      log.info("field str key is " + str);
    }
  }

  // 异步方式执行ExplainRequest
  public void executeExplainRequestAsync(String indexName, String document, String field,
      String content) {
    // 构建ExplainRequest请求
    ExplainRequest request = buildExplainRequest(indexName, document, field, content);

    // 构建监听器
    ActionListener<ExplainResponse> listener = new ActionListener<ExplainResponse>() {
      @Override
      public void onResponse(ExplainResponse explainResponse) {

      }

      @Override
      public void onFailure(Exception e) {

      }
    };


    // 执行请求，接收返回结果
    try {
      restClient.explainAsync(request, RequestOptions.DEFAULT, listener);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 关闭ES的连接
      closeEs();
    }
  }
}
