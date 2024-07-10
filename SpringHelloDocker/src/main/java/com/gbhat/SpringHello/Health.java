package com.gbhat.SpringHello;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
public class Health {
    @GetMapping("/healthcheck")
    public String healthcheck() {
	System.out.println("Health checked at " + new Date());
        return "Healthy at " + new Date();
    }
}
