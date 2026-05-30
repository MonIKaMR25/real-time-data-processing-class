-- Lesson 3: NYC Taxi trips table for OLAP benchmarks
CREATE TABLE IF NOT EXISTS trips (
    vendor_id            INT,
    pickup_datetime      TIMESTAMPTZ,
    dropoff_datetime     TIMESTAMPTZ,
    passenger_count      INT,
    trip_distance        NUMERIC(10, 2),
    pickup_location_id   INT,
    dropoff_location_id  INT,
    rate_code_id         INT,
    payment_type         INT,
    fare_amount          NUMERIC(10, 2),
    extra                NUMERIC(10, 2),
    mta_tax              NUMERIC(10, 2),
    tip_amount           NUMERIC(10, 2),
    tolls_amount         NUMERIC(10, 2),
    total_amount         NUMERIC(10, 2),
    congestion_surcharge NUMERIC(10, 2),
    airport_fee          NUMERIC(10, 2)
);

-- Index for point-query experiment (Experiment A)
CREATE INDEX IF NOT EXISTS idx_trips_pickup ON trips (pickup_datetime);
