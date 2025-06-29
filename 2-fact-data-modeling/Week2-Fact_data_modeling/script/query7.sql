CREATE TABLE public.host_activity_reduced (
    month INTEGER,
    host VARCHAR,
    hit_array INTEGER[],
    unique_visitor TEXT[],
    --data_date
    date DATE
)