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
	CONSTRAINT fk_source_files 
	FOREIGN KEY (source_file) 
	REFERENCES source_files(id) 
	ON DELETE CASCADE
);

create table suicide_age (
	id integer PRIMARY KEY autoincrement,
	age varchar(255) NOT NULL,
	sex integer NOT NULL,
	suicides_no integer NOT NULL,
	population integer NOT NULL,
	suicides_on_100KPopulation integer NOT NULL,
	source_file integer NOT NULL,
	CONSTRAINT fk_source_files 
	FOREIGN KEY (source_file) 
	REFERENCES source_files(id) 
	ON DELETE CASCADE
);
	 