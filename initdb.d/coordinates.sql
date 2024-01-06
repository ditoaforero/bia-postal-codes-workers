CREATE TABLE coordinates (
    lat float8 null,
    lon float8 null,
    postcode varchar(10) null,
    primary key (lat, lon)
);