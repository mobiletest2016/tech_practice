package com.example;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping(path = "/world")
public class MainController {

    @Autowired
    private CountryRepository countryRepository;

    @GetMapping(path = "/countrylist")
    public @ResponseBody
    Iterable<Country> getAllCountries() {
	return countryRepository.findAll();
    }

    @GetMapping(path = "/countrydetails/{code}")
    public @ResponseBody
    Country getCountryDetails(@PathVariable String code) {
	return countryRepository.findByCode(code);
    }

    @GetMapping(path = "/citylist")
    public @ResponseBody
    List<City> getCityForCountry(@RequestParam String countrycode) {
	return countryRepository.findByCode(countrycode).getCityList();
    }

    @GetMapping(path = "/languagelist")
    public @ResponseBody
    List<Language> getLanguagesForCountry(@RequestParam String countrycode) {
        return countryRepository.findByCode(countrycode).getLanguageList();
    }
}
