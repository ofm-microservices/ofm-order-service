ALTER TABLE order_gig_snapshot
ADD COLUMN IF NOT EXISTS picture_file_id TEXT;

UPDATE order_gig_snapshot
SET picture_file_id = COALESCE(picture_file_id, '')
WHERE picture_file_id IS NULL;
