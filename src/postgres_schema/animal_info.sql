CREATE TABLE animal_info(
   id INTEGER PRIMARY KEY,
   organization INTEGER ,
   name Char(100) ,
   size INTEGER ,
   age INTEGER ,
   gender CHAR(50),
   breeds_primary CHAR(100),
   breeds_secondary CHAR(100),
   breeds_mixed CHAR(100),
   breeds_unknown CHAR(100),
   colors_primary CHAR(100),
   colors_secondary CHAR(100),
   colors_mixed CHAR(100),
   coat CHAR(100),
   published_at TIMESTAMP,
--    FOREIGN KEY (animal_id) REFERENCES animal_description(id)
);


-- size is actually a string not an integer