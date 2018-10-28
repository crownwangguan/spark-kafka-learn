#Create new mysql user

mysql -u root -pcloudera

GRANT ALL ON *.* to cloudera@'%' IDENTIFIED BY 'cloudera';

flush privileges;

exit;

#Login as user and create the sample table.

mysql -u cloudera -pcloudera



#Execute the following commands within mysql



create database jdbctest;

use jdbctest;



CREATE TABLE jdbctest.jdbc_source (

   TEST_ID INT AUTO_INCREMENT,

   `TEST_TIMESTAMP` TIMESTAMP,

  PRIMARY KEY (TEST_ID)

) ENGINE = InnoDB ROW_FORMAT = DEFAULT;



#Use this command to create any number of records in the table.

insert into jdbc_source(TEST_TIMESTAMP) values ( now());

insert into jdbc_source(TEST_TIMESTAMP) values ( now());

insert into jdbc_source(TEST_TIMESTAMP) values ( now());

insert into jdbc_source(TEST_TIMESTAMP) values ( now());

