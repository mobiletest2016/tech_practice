DROP TABLE IF EXISTS Country;
CREATE TABLE Country (
  Code CHAR(3) NOT NULL DEFAULT '',
  Name VARCHAR NOT NULL DEFAULT '',
  Continent enum('Asia','Europe','North America','Africa','Oceania','Antarctica','South America') NOT NULL DEFAULT 'Asia',
  Region VARCHAR NOT NULL DEFAULT '',
  SurfaceArea FLOAT NOT NULL DEFAULT '0.00',
  IndepYear SMALLINT DEFAULT NULL,
  Population INT NOT NULL DEFAULT '0',
  LifeExpectancy FLOAT DEFAULT NULL,
  GNP FLOAT DEFAULT NULL,
  GNPOld FLOAT DEFAULT NULL,
  LocalName VARCHAR NOT NULL DEFAULT '',
  GovernmentForm VARCHAR NOT NULL DEFAULT '',
  HeadOfState VARCHAR DEFAULT NULL,
  Capital INT DEFAULT NULL,
  Code2 CHAR(2) NOT NULL DEFAULT '',
  PRIMARY KEY (Code)
);

DROP TABLE IF EXISTS City;
CREATE TABLE City (
  ID INT NOT NULL AUTO_INCREMENT,
  Name VARCHAR NOT NULL DEFAULT '',
  CountryCode CHAR(3) NOT NULL DEFAULT '',
  District VARCHAR NOT NULL DEFAULT '',
  Population INT NOT NULL DEFAULT '0',
  PRIMARY KEY (ID),
  FOREIGN KEY (CountryCode) REFERENCES Country (Code)
);

DROP TABLE IF EXISTS CountryLanguage;
CREATE TABLE CountryLanguage (
  CountryCode CHAR(3) NOT NULL DEFAULT '',
  Language VARCHAR NOT NULL DEFAULT '',
  IsOfficial enum('T','F') NOT NULL DEFAULT 'F',
  Percentage FLOAT NOT NULL DEFAULT '0.0',
  PRIMARY KEY (CountryCode,Language),
  FOREIGN KEY (CountryCode) REFERENCES Country (Code)
);
