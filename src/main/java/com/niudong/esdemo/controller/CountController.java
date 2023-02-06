package com.niudong.esdemo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.niudong.esdemo.service.CountService;

/**
 * 
 * @author 牛冬
 * @desc:本类用于描述搜索统计相关API使用和测试
 *
 */
@RestController
@RequestMapping("/springboot/es/countsearch")
public class CountController {
  @Autowired
  private CountService countService;

  // 同步方式执行CountRequest
  @RequestMapping("/sr")
  public String executeCount() {
    countService.executeCountRequest();

    return "Execute CountRequest success!";
  }

}
