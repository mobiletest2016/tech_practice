package com.example;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
@Data
public class City {

    @Id
    private String id;
    private String name, countryCode;
    int population;
}
