create table source_files (
	id integer PRIMARY KEY autoincrement, 
	filename varchar(255) NOT NULL, 
	processed datetime
);

create table suicides_country (
	id integer PRIMARY KEY autoincrement,
	country varchar(255) NOT NULL,
	sex integer NOT NULL,
	gdp_per_capita integer NOT NULL,
	suicides_no integer NOT NULL,
	population integer NOT NULL,
	suicides_on_100KPopulation integer NOT NULL,
	Level_gdp integer NOT NULL,
	source_file integer NOT NULL,
	CONSTRAINT fk_sex 
	FOREIGN KEY (sex)
	REFERENCES index_sex(id)
	CONSTRAINT Level_gdp 
	FOREIGN KEY (index_gdp)
	REFERENCES index_gdp(id)
	CONSTRAINT fk_source_files 
	FOREIGN KEY (source_file) 
	REFERENCES source_files(id) 
	ON DELETE CASCADE
);

create table suicides_year (
	id integer PRIMARY KEY autoincrement,
	year integer NOT NULL,
	sex integer NOT NULL,
	suicides_no integer NOT NULL,
	population integer NOT NULL,
	suicides_on_100KPopulation integer NOT NULL,
	source_file integer NOT NULL,
	CONSTRAINT fk_sex 
	FOREIGN KEY (sex)
	REFERENCES index_sex(id))
	CONSTRAINT fk_source_files 
	FOREIGN KEY (source_file) 
	REFERENCES source_files(id) 
	ON DELETE CASCADE
);

create table suicides_age (
	id integer PRIMARY KEY autoincrement,
	age varchar(255) NOT NULL,
	sex integer NOT NULL,
	suicides_no integer NOT NULL,
	population integer NOT NULL,
	suicides_on_100KPopulation integer NOT NULL,
	source_file integer NOT NULL,
	CONSTRAINT fk_sex 
	FOREIGN KEY (sex)
	REFERENCES index_sex(id)
	CONSTRAINT fk_source_files 
	FOREIGN KEY (source_file) 
	REFERENCES source_files(id) 
	ON DELETE CASCADE
);

create table index_sex (
	id integer PRIMARY KEY autoincrement, 
	sex varchar(6) NOT NULL
);

insert into index_sex (name) VALUES ("man")
insert into index_sex (name) VALUES ("woman")
	 
create table index_gdp (
	id integer PRIMARY KEY autoincrement, 
	level_gdp varchar(255) NOT NULL 
);

insert into index_gdp (name) VALUES ("High")
insert into index_gdp (name) VALUES ("Medium")
insert into index_gdp (name) VALUES ("Low")

SELECT sc.id, country, ins.sex, gdp_per_capita, ing.level_gdp, suicides_no, population, suicides_on_100KPopulation, source_file  
FROM suicides_country sc, index_gdp ing, index_sex ins WHERE sc.Level_gdp = ing.id AND ins.id = sc.sex 

SELECT sy.id, sy."year", ins.sex, suicides_no, population, suicides_on_100KPopulation, source_file  
FROM suicides_year sy, index_sex ins WHERE ins.id = sy.sex 

SELECT sa.id, sa.age, ins.sex, suicides_no, population, suicides_on_100KPopulation, source_file  
FROM suicides_age sa, index_sex ins WHERE ins.id = sa.sex 
