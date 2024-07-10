package com.gbhat.SpringHello;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Shutdown {

    @Autowired
    private ApplicationContext applicationContext;

    @GetMapping("/shutdown")
    public void shutdown() {
        ((ConfigurableApplicationContext) applicationContext).close();
    }
}
