CREATE TABLE IF NOT EXISTS Station (
    station_id bigint not null,
    station_name text not null,
    station_description text not null,
    station_latitude float not null,
    station_longitude float not null,
    primary key(station_id)
)