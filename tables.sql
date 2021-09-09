CREATE TABLE Page (
    name text PRIMARY KEY
);

CREATE TABLE Influenced (
    name text,
	influenced text, 
	FOREIGN KEY (name) REFERENCES Page(name),
    FOREIGN KEY (influenced) REFERENCES Page(name),
    CONSTRAINT PK_Influenced PRIMARY KEY (name, influenced)
);
