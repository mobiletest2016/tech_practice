package com.example;

import org.springframework.data.repository.CrudRepository;

public interface CountryRepository extends CrudRepository<Country, Long> {

    Country findByName(String name);

    Country findByCode(String code);
}
