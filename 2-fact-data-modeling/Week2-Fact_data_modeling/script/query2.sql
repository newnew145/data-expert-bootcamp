CREATE TABLE public.user_devices_cumulated (
-- using text for user_id becuase user_id out of range
    user_id TEXT,
	-- using jsonb for browser_type:datelist_arr
    device_activity_datelist jsonb,
	-- keep date array
	dates_active DATE[],
    date DATE,
	PRIMARY KEY (user_id ,date)
)
