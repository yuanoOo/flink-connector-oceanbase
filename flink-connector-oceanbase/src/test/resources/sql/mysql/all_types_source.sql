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

CREATE TABLE all_types_source
(
  id             INT            NOT NULL PRIMARY KEY,
  col_boolean    BOOLEAN,
  col_tinyint    TINYINT,
  col_smallint   SMALLINT,
  col_int        INT,
  col_bigint     BIGINT,
  col_float      FLOAT,
  col_double     DOUBLE,
  col_decimal    DECIMAL(20, 10),
  col_char       CHAR(10),
  col_varchar    VARCHAR(255),
  col_date       DATE,
  col_time       TIME,
  col_timestamp  TIMESTAMP      NULL,
  col_binary     BINARY(16),
  col_varbinary  VARBINARY(255)
);

INSERT INTO all_types_source VALUES
(1, true, 127, 32767, 2147483647, 9223372036854775807, 3.14, 3.14159265358979, 12345.6789012345, 'hello     ', 'world', '2025-01-15', '12:34:56', '2025-01-15 10:30:00', 'Hello', 'Flink');

INSERT INTO all_types_source VALUES
(2, false, -128, -32768, -2147483648, -9223372036854775808, -1.5, -2.718281828, 99999.9999999999, 'test      ', 'OceanBase', '2000-06-30', '23:59:59', '2000-12-31 23:59:59', 'OceanBase', 'OB');

INSERT INTO all_types_source VALUES
(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE all_types_sink
(
  id             INT            NOT NULL PRIMARY KEY,
  col_boolean    BOOLEAN,
  col_tinyint    TINYINT,
  col_smallint   SMALLINT,
  col_int        INT,
  col_bigint     BIGINT,
  col_float      FLOAT,
  col_double     DOUBLE,
  col_decimal    DECIMAL(20, 10),
  col_char       CHAR(10),
  col_varchar    VARCHAR(255),
  col_date       DATE,
  col_time       TIME,
  col_timestamp  TIMESTAMP      NULL,
  col_binary     BINARY(16),
  col_varbinary  VARBINARY(255)
);
