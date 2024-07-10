package com.gbhat.SpringHello;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
public class HelloREST {
    Logger logger = LoggerFactory.getLogger(HelloREST.class);

    @GetMapping("/")
    public String hello() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "Unknown Host";
        }
        logger.error("Received a request at " + new Date());
        return "Hello from Host: " + hostname + " Time: " + new Date();
    }
}
