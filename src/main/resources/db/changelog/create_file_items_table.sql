CREATE TABLE public.file_items (
	recorduniqueid varchar NOT NULL,
	batchid varchar NULL,
	accountnumber varchar NULL,
	itemstatus varchar NULL,
	payload varchar NULL,
	CONSTRAINT file_items_pk PRIMARY KEY (recorduniqueid)
);