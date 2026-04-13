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

CREATE TABLE string_pk_source
(
  code    VARCHAR(50) PRIMARY KEY,
  name    VARCHAR(255),
  price   DECIMAL(10, 2)
);

INSERT INTO string_pk_source VALUES ('A001', 'Alpha', 10.50);
INSERT INTO string_pk_source VALUES ('B002', 'Bravo', 20.00);
INSERT INTO string_pk_source VALUES ('C003', 'Charlie', 30.75);
INSERT INTO string_pk_source VALUES ('D004', 'Delta', 40.25);
INSERT INTO string_pk_source VALUES ('E005', 'Echo', 50.00);
INSERT INTO string_pk_source VALUES ('F006', 'Foxtrot', 60.80);
INSERT INTO string_pk_source VALUES ('G007', 'Golf', 70.10);
INSERT INTO string_pk_source VALUES ('H008', 'Hotel', 80.90);
INSERT INTO string_pk_source VALUES ('I009', 'India', 90.45);
INSERT INTO string_pk_source VALUES ('J010', 'Juliet', 100.00);

CREATE TABLE string_pk_sink
(
  code    VARCHAR(50) PRIMARY KEY,
  name    VARCHAR(255),
  price   DECIMAL(10, 2)
);
