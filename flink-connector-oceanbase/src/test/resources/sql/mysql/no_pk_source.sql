-- Copyright 2024 OceanBase.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

CREATE TABLE no_pk_source
(
  name    VARCHAR(255),
  value   INT
);

INSERT INTO no_pk_source VALUES ('a', 1);
INSERT INTO no_pk_source VALUES ('b', 2);
INSERT INTO no_pk_source VALUES ('c', 3);
INSERT INTO no_pk_source VALUES ('d', 4);
INSERT INTO no_pk_source VALUES ('e', 5);
INSERT INTO no_pk_source VALUES ('f', 6);
INSERT INTO no_pk_source VALUES ('g', 7);
INSERT INTO no_pk_source VALUES ('h', 8);
INSERT INTO no_pk_source VALUES ('i', 9);
INSERT INTO no_pk_source VALUES ('j', 10);

CREATE TABLE no_pk_sink
(
  name    VARCHAR(255),
  value   INT
);
