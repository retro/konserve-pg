-- :name db-create-table 
-- :command :execute
-- :result :raw
CREATE TABLE IF NOT EXISTS __konserve_data (
  id text UNIQUE,
  edn_value bytea,
  attachment bytea
);
CREATE INDEX IF NOT EXISTS __konserve_data_id ON __konserve_data USING btree (id);

-- :name db-drop-table
-- :command :execute
-- :result :raw
DROP TABLE IF EXISTS __konserve_data CASCADE;

-- :name db-record-exists
SELECT EXISTS(SELECT 1 FROM __konserve_data WHERE id = :id)

-- :name db-get-record-edn-value
SELECT id, edn_value FROM __konserve_data WHERE id = :id LIMIT 1

-- :name db-get-record-attachment
SELECT id, attachment FROM __konserve_data WHERE id = :id LIMIT 1

-- :name db-delete-record :! :n
DELETE FROM __konserve_data WHERE id = :id

-- :name db-upsert-record-edn-value
INSERT INTO __konserve_data (id, edn_value) VALUES(:id, :edn-value)
ON CONFLICT (id)
   DO UPDATE SET
      edn_value = :edn-value,
      attachment = NULL
RETURNING *;

-- :name db-upsert-record-attachment
INSERT INTO __konserve_data (id, attachment) VALUES(:id, :attachment)
ON CONFLICT (id)
   DO UPDATE SET
      attachment = :attachment,
      edn_value = NULL
RETURNING *;
