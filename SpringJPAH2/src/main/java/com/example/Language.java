package com.example;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Data
@Table(name = "CountryLanguage")
public class Language implements Serializable {

    @Id
    private String language, countryCode;
    private String isOfficial;
    @Column(name="percentage")
    private float percent;
}
