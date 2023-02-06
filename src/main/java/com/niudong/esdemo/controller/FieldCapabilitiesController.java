package com.niudong.esdemo.controller;

import java.util.List;

import org.elasticsearch.common.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.base.Splitter;
import com.niudong.esdemo.service.FieldCapabilitiesService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述跨索引字段搜索相关API使用和测试
 *
 */
@RestController
@RequestMapping("/springboot/es/fieldsearch")
public class FieldCapabilitiesController {
  @Autowired
  private FieldCapabilitiesService fieldCapabilitiesService;
  
  // 同步方式执行MultiSearchRequest
  @RequestMapping("/sr")
  public String executeFieldSearchRequest(String field, String indices) {
    // 参数校验
    if (Strings.isNullOrEmpty(field) || Strings.isNullOrEmpty(indices)) {
      return "Parameters are wrong!";
    }

    // 将英文逗号分隔的字符串切分成数组
    List<String> indicesList = Splitter.on(",").splitToList(indices);
    fieldCapabilitiesService.executeFieldCapabilitiesRequest(field,
        indicesList.toArray(new String[indicesList.size()]));
    return "Execute FieldSearchRequest success!";
  }
}
