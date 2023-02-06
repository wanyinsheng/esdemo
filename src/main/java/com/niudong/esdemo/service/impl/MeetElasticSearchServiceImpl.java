package com.niudong.esdemo.service.impl;

import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyStore;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.client.sniff.NodesSniffer;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.springframework.stereotype.Service;
import com.niudong.esdemo.service.MeetElasticSearchService;

/**
 * 
 * @author 牛冬
 * @desc:ES低级客户端初始化相关操作
 *
 */
@Service
public class MeetElasticSearchServiceImpl implements MeetElasticSearchService {
  private static Log log = LogFactory.getLog(MeetElasticSearchServiceImpl.class);
  private RestClient restClient;

  /**
   * 本部分用于介绍如何与ElasticSearch构建连接和关闭连接
   */

  // 初始化连接
  @PostConstruct
  public void initEs() {
    initEsWithFail();
//    restClient = RestClient
//        .builder(new HttpHost("192.168.56.111", 9200, "http"), new HttpHost("192.168.56.111", 9201, "http"))
//        .build();
//    log.info("ElasticSearch init in service.");
  }

  // 关闭连接
  public void closeEs() {
    try {
      restClient.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // 带请求头
  public void initEsWithHeader() {
    RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
    // 设置每个请求需要发送的默认头，以防止在每个请求中指定它们。
    Header[] defaultHeaders = new Header[] {new BasicHeader("header", "value")};
    builder.setDefaultHeaders(defaultHeaders);
  }

  // 带失败监听
  public void initEsWithFail() {
    RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
    // 设置一个侦听器，该侦听器在每次节点失败时都会收到通知，以防需要采取操作。在启用故障嗅探时在内部使用。
    builder.setFailureListener(new RestClient.FailureListener() {
      @Override
      public void onFailure(Node node) {
        System.out.println("#################################3");
      }
    });
  }

  // 带节点选择器
  public void initEsWithNodeSelector() {
    RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
    builder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
  }

  // 设置超时时间
  public void initEsWithTimeout() {
    RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
    builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
      @Override
      public RequestConfig.Builder customizeRequestConfig(
          RequestConfig.Builder requestConfigBuilder) {
        return requestConfigBuilder.setSocketTimeout(10000).setSocketTimeout(60000);
      }
    });
  }

  /**
   * 本部分用于介绍如何构建对ElasticSearch服务的请求
   */
  public Request buildRequest() {
    Request request = new Request("GET", // 同The HTTP 请求方式，如GET、 POST、 HEAD
        "/");
    return request;
  }

  /**
   * 本部分用于介绍如何构建对ElasticSearch服务的请求
   */
  public String executeRequest() {
    Request request = new Request("GET", // 同The HTTP 请求方式，如GET、 POST、 HEAD
        "/");

    // 在服务器上请求
    try {
      Response response = restClient.performRequest(request);
      return response.toString();
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      restClient.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return "Get result failed!";
  }

  // 异步在服务器上请求
  public String executeRequestAsync() {
    Request request = new Request("GET", // 同The HTTP 请求方式，如GET、 POST、 HEAD
        "/");

    // 在服务器上请求
    restClient.performRequestAsync(request, new ResponseListener() {
      @Override
      public void onSuccess(Response response) {

      }

      @Override
      public void onFailure(Exception exception) {

      }
    });

    try {
      restClient.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return "Get result failed!";
  }

  // 设置全局单例RequestOptions
  private static final RequestOptions COMMON_OPTIONS;
  static {
    RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
    builder.addHeader("Authorization", "Bearer " + "my-token");
    builder.setHttpAsyncResponseConsumerFactory(
        new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(
            30 * 1024 * 1024 * 1024));
    COMMON_OPTIONS = builder.build();
  }

  /**
   * 本部分用于介绍如何构建对ElasticSearch服务的请求
   */
  public String buildRequestWithRequestOptions() {
    Request request = new Request("GET", // 同The HTTP 请求方式，如GET、 POST、 HEAD
        "/");

    // 在服务器上请求
    try {
      Response response = restClient.performRequest(request);

      RequestOptions.Builder options = COMMON_OPTIONS.toBuilder();
      options.addHeader("title", "u r my dear!");
      request.setOptions(COMMON_OPTIONS);

      return response.toString();
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      restClient.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return "Get result failed!";
  }

  // 并发处理文档数据
  public void multiDocumentProcess(HttpEntity[] documents) {
    final CountDownLatch latch = new CountDownLatch(documents.length);
    for (int i = 0; i < documents.length; i++) {
      Request request = new Request("PUT", "/posts/doc/" + i);
      // 假设 documents 存储在 HttpEntity 数组中
      request.setEntity(documents[i]);
      restClient.performRequestAsync(request, new ResponseListener() {
        @Override
        public void onSuccess(Response response) {

          latch.countDown();
        }

        @Override
        public void onFailure(Exception exception) {

          latch.countDown();
        }
      });
    }

    try {
      latch.await();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 本部分用于介绍如何解析ElasticSearch服务的返回结果
   */
  public void parseElasticSearchResponse() {
    try {
      Response response = restClient.performRequest(new Request("GET", "/"));

      // 有关已执行请求的信息
      RequestLine requestLine = response.getRequestLine();
      // Host返回的信息
      HttpHost host = response.getHost();
      // 响应状态行，从中可以解析状态代码
      int statusCode = response.getStatusLine().getStatusCode();
      // 响应头，也可以通过getheader（string）按名称获取
      Header[] headers = response.getHeaders();
      String responseBody = EntityUtils.toString(response.getEntity());

      log.info("parse ElasticSearch Response,responseBody is :" + responseBody);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 本部分用于介绍使用ElasticSearch客户端的通用设置
   */
  public void setThreadNumber(int number) {
    RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200))
        .setHttpClientConfigCallback(new HttpClientConfigCallback() {
          @Override
          public HttpAsyncClientBuilder customizeHttpClient(
              HttpAsyncClientBuilder httpClientBuilder) {
            return httpClientBuilder.setDefaultIOReactorConfig(
                IOReactorConfig.custom().setIoThreadCount(number).build());
          }
        });
  }

  // 设置节点选择器
  public void setNodeSelector() {
    RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
    builder.setNodeSelector(new NodeSelector() {
      @Override
      public void select(Iterable<Node> nodes) {
        boolean foundOne = false;
        for (Node node : nodes) {
          String rackId = node.getAttributes().get("rack_id").get(0);
          if ("targetId".equals(rackId)) {
            foundOne = true;
            break;
          }
        }

        if (foundOne) {
          Iterator<Node> nodesIt = nodes.iterator();
          while (nodesIt.hasNext()) {
            Node node = nodesIt.next();
            String rackId = node.getAttributes().get("rack_id").get(0);
            if ("targetId".equals(rackId) == false) {
              nodesIt.remove();
            }
          }
        }

      }
    });
  }

  // 配置嗅探器
  public void setSniffer() {
    RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();
    Sniffer sniffer = Sniffer.builder(restClient)
        // 嗅探器默认每5分钟更新一次节点。可以通过setSniffIntervalMillis（以毫秒为单位）自定义此间隔
        .setSniffIntervalMillis(60000).build();

    // 使用完毕结束客户端和嗅探器
    try {
      sniffer.close();
      restClient.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // 配置嗅探器
  public void setSnifferWhenFail(int failTime) {
    SniffOnFailureListener sniffOnFailureListener = new SniffOnFailureListener();
    RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200))
        .setFailureListener(sniffOnFailureListener).build();
    Sniffer sniffer = Sniffer.builder(restClient).setSniffAfterFailureDelayMillis(failTime).build();
    sniffOnFailureListener.setSniffer(sniffer);

    // 使用完毕结束客户端和嗅探器
    try {
      sniffer.close();
      restClient.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // 配置基于HTTPS协议的嗅探器
  public void setSnifferWithHTTPS(int failTime) {
    RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();
    NodesSniffer nodesSniffer = new ElasticsearchNodesSniffer(restClient,
        ElasticsearchNodesSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT,
        ElasticsearchNodesSniffer.Scheme.HTTPS);
    Sniffer sniffer = Sniffer.builder(restClient).setNodesSniffer(nodesSniffer).build();

    // 使用完毕结束客户端和嗅探器
    try {
      sniffer.close();
      restClient.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
