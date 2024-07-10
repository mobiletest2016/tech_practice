package com.gbhat.SpringHello;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
public class Health {
    Logger logger = LoggerFactory.getLogger(Health.class);

    @GetMapping("/healthcheck")
    public String healthcheck() {
        logger.error("Health checked at " + new Date());
        return "Healthy at " + new Date();
    }
}
