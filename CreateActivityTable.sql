CREATE TABLE IF NOT EXISTS Activity (
    started_at timestamptz not null,
    ended_at timestamptz not null,
    duration bigint not null,
    start_station_id bigint not null,
    end_station_id bigint not null
)