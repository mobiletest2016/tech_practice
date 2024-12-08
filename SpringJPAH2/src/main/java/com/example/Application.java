package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/*
Visit:
    http://localhost:8080/world/countrylist
    http://localhost:8080/world/countrydetails/IND
    http://localhost:8080/world/citylist?countrycode=IND
    http://localhost:8080/world/languagelist?countrycode=IND
 */
@SpringBootApplication
public class Application {

    public static void main(String[] args) {
	SpringApplication.run(Application.class, args);
    }
}
