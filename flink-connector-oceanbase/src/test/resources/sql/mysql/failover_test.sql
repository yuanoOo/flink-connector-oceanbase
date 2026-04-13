-- Copyright 2024 OceanBase.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE IF NOT EXISTS failover_products (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    description VARCHAR(512),
    weight DECIMAL(20, 10)
);

INSERT INTO failover_products VALUES (1, 'prod_01', 'Description for product 1', 1.0000000000);
INSERT INTO failover_products VALUES (2, 'prod_02', 'Description for product 2', 2.0000000000);
INSERT INTO failover_products VALUES (3, 'prod_03', 'Description for product 3', 3.0000000000);
INSERT INTO failover_products VALUES (4, 'prod_04', 'Description for product 4', 4.0000000000);
INSERT INTO failover_products VALUES (5, 'prod_05', 'Description for product 5', 5.0000000000);
INSERT INTO failover_products VALUES (6, 'prod_06', 'Description for product 6', 6.0000000000);
INSERT INTO failover_products VALUES (7, 'prod_07', 'Description for product 7', 7.0000000000);
INSERT INTO failover_products VALUES (8, 'prod_08', 'Description for product 8', 8.0000000000);
INSERT INTO failover_products VALUES (9, 'prod_09', 'Description for product 9', 9.0000000000);
INSERT INTO failover_products VALUES (10, 'prod_10', 'Description for product 10', 10.0000000000);
INSERT INTO failover_products VALUES (11, 'prod_11', 'Description for product 11', 11.0000000000);
INSERT INTO failover_products VALUES (12, 'prod_12', 'Description for product 12', 12.0000000000);
INSERT INTO failover_products VALUES (13, 'prod_13', 'Description for product 13', 13.0000000000);
INSERT INTO failover_products VALUES (14, 'prod_14', 'Description for product 14', 14.0000000000);
INSERT INTO failover_products VALUES (15, 'prod_15', 'Description for product 15', 15.0000000000);
INSERT INTO failover_products VALUES (16, 'prod_16', 'Description for product 16', 16.0000000000);
INSERT INTO failover_products VALUES (17, 'prod_17', 'Description for product 17', 17.0000000000);
INSERT INTO failover_products VALUES (18, 'prod_18', 'Description for product 18', 18.0000000000);
INSERT INTO failover_products VALUES (19, 'prod_19', 'Description for product 19', 19.0000000000);
INSERT INTO failover_products VALUES (20, 'prod_20', 'Description for product 20', 20.0000000000);
INSERT INTO failover_products VALUES (21, 'prod_21', 'Description for product 21', 21.0000000000);
INSERT INTO failover_products VALUES (22, 'prod_22', 'Description for product 22', 22.0000000000);
INSERT INTO failover_products VALUES (23, 'prod_23', 'Description for product 23', 23.0000000000);
INSERT INTO failover_products VALUES (24, 'prod_24', 'Description for product 24', 24.0000000000);
INSERT INTO failover_products VALUES (25, 'prod_25', 'Description for product 25', 25.0000000000);
INSERT INTO failover_products VALUES (26, 'prod_26', 'Description for product 26', 26.0000000000);
INSERT INTO failover_products VALUES (27, 'prod_27', 'Description for product 27', 27.0000000000);
INSERT INTO failover_products VALUES (28, 'prod_28', 'Description for product 28', 28.0000000000);
INSERT INTO failover_products VALUES (29, 'prod_29', 'Description for product 29', 29.0000000000);
INSERT INTO failover_products VALUES (30, 'prod_30', 'Description for product 30', 30.0000000000);
