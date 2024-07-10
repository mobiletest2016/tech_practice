package com.gbhat.SpringHello;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

@RestController
public class HelloREST {
    @GetMapping("/")
    public String hello() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "Unknown Host";
        }
	System.out.println("Received a request at " + new Date());
        return "Hello from Host: " + hostname + " Time: " + new Date();
    }
}
