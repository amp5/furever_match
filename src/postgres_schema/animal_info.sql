CREATE TABLE animal_info(
   animal_id INTEGER PRIMARY KEY,
   organization INTEGER ,
   name Char(100) ,
   size INTEGER ,
   age INTEGER ,
   gender CHAR(50),
   breed_primary CHAR(100),
   breed_secondary CHAR(100),
   breed_mixed CHAR(100),
   breed_unknown CHAR(100),
   color_primary CHAR(100),
   color_secondary CHAR(100),
   color_tertiary CHAR(100),
   coat CHAR(100),
   date_added TIMESTAMP,
--    FOREIGN KEY (animal_id) REFERENCES animal_description(id)
);


