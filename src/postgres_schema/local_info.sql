CREATE TABLE local_info(
   local_id CHAR(20) PRIMARY KEY,
   name Char(100) ,
   age INTEGER,
   fixed_status CHAR(20),
   gender CHAR(20),
   breed CHAR(100),
   color CHAR(100),
   zip CHAR(10),
   memo CHAR(2000),
   temperament CHAR(20),
   age_group CHAR(20)
--    FOREIGN KEY (animal_id) REFERENCES animal_description(id)
);
