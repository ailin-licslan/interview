package com.licslan.interview.test;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WebReq {

    @GetMapping("/")
    public String test(){
        return "hello world!";
    }
}
