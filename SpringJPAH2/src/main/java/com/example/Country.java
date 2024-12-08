package com.example;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.List;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToMany;

@Entity
@Data
public class Country {

    @Id
    private String code;

    private String name, continent, region;

    private int population;

    @JsonIgnore
    @OneToMany(mappedBy = "countryCode")
    List<City> cityList;

    @JsonIgnore
    @OneToMany(mappedBy = "countryCode")
    List<Language> languageList;
}
