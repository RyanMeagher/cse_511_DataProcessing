--CREATE a movie database. 



CREATE TABLE users(
					userid int PRIMARY KEY,
					name varchar UNIQUE NOT NULL
					);

CREATE TABLE movies(movieid integer PRIMARY KEY,
					title varchar  NOT NULL
					);


CREATE TABLE taginfo(
					tagid int PRIMARY KEY,
					content varchar
					);

CREATE TABLE genres(genreid int PRIMARY KEY,
					name varchar UNIQUE NOT NULL 
					);


CREATE TABLE ratings(userid int REFERENCES users(userid),
					movieid int REFERENCES movies(movieid),
					rating numeric CHECK (rating>=0) CHECK (rating<=5),
					timestamp bigint,  
					CONSTRAINT usr_ratings_pkey PRIMARY KEY (userid,movieid)
					);


CREATE TABLE tags (userid int REFERENCES users(userid) , 
	 			   movieid int REFERENCES movies(movieid),
				   tagid int REFERENCES taginfo(tagid), 
					timestamp bigint 
					);



CREATE TABLE hasagenre(movieid int NOT NULL REFERENCES movies(movieid),
					   genreid int NOT NULL REFERENCES genres(genreid),
					   CONSTRAINT hasha_pkey PRIMARY KEY(movieid, genreid)
					);