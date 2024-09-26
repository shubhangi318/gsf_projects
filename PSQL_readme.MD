# PSQL Commands to Store and Normalize Data

This readme contains PostgreSQL (PSQL) commands used for creating schemas, tables, and managing data insertion and normalization.

## Table of Contents
1. [Schema Creation](#schema-creation)
2. [Table Definitions](#table-definitions)
   - [Main Project Details](#main-project-details)
   - [Sustainable Development Goals](#sustainable-development-goals)
   - [Link Table](#link-table)
   - [Project Description](#project-description)
3. [Data Insertion](#data-insertion)
4. [Normalization](#normalization)
5. [Project Description](#project-description-population)

## Schema Creation

To create the schema `gsf_reg`, execute the following command:

```sql
CREATE SCHEMA gsf_reg;
```

## Table Definitions

Main Project Details

```sql
CREATE TABLE gsf_reg.main_proj_details (
    id                            INTEGER NOT NULL,
    created_at                    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at                    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    name                          CHARACTER VARYING NOT NULL,
    description                   CHARACTER VARYING,
    status                        CHARACTER VARYING,
    gsf_standards_version         CHARACTER VARYING,
    estimated_annual_credits      INTEGER,
    crediting_period_start_date   TIMESTAMP WITHOUT TIME ZONE,
    crediting_period_end_date     TIMESTAMP WITHOUT TIME ZONE,
    methodology                   CHARACTER VARYING,
    type                          CHARACTER VARYING,
    size                          CHARACTER VARYING,
    sustaincert_id                INTEGER,
    sustaincert_url               JSONB,
    project_developer             CHARACTER VARYING,
    carbon_stream                 CHARACTER VARYING,
    country                       CHARACTER VARYING,
    country_code                  CHARACTER VARYING,
    state                         CHARACTER VARYING,
    programme_of_activities       CHARACTER VARYING,
    poa_project_id                NUMERIC,
    poa_project_sustaincert_id     NUMERIC,
    poa_project_name              CHARACTER VARYING,
    sustainable_development_goals  JSONB,
    labels                        JSONB,
    hsh                           CHARACTER VARYING
);

-- Create an index on the hsh column
CREATE INDEX idx_hsh ON gsf_reg.main_proj_details(hsh);
```

Sustainable Development Goals

```sql
CREATE TABLE gsf_reg.sust_goals_details (
    goal_name VARCHAR,
    issuable_products JSONB,
    goal_id INTEGER
);

```
Link Table
```sql
CREATE TABLE gsf_reg.sust_goal_link (
    proj_hash VARCHAR,
    goal_id INT
);

```
Project Description

```sql
CREATE TABLE gsf_reg.proj_desc (
    description              CHARACTER VARYING,
    project_developer        CHARACTER VARYING,
    annual_est_credits       INTEGER,
    project_scale            CHARACTER VARYING,
    methodology              CHARACTER VARYING,
    credit_period_start_date TIMESTAMP WITHOUT TIME ZONE,
    credit_period_end_date   TIMESTAMP WITHOUT TIME ZONE,
    proj_hash                CHARACTER VARYING,

    -- Define the unique constraint
    CONSTRAINT proj_desc_proj_hash_key UNIQUE (proj_hash)
);

-- Create an index on the proj_hash column
CREATE INDEX idx_proj_desc_proj_hash ON gsf_reg.proj_desc(proj_hash);
```

## Data Insertion

After extracting the project details into main_project.csv, use the following command to insert the data into the main_proj_details table:

```sql
\copy gsf_reg.main_proj_details(id, created_at, updated_at, name, description, status, gsf_standards_version, estimated_annual_credits, crediting_period_start_date, crediting_period_end_date, methodology, type, size, sustaincert_id, sustaincert_url, project_developer, carbon_stream, country, country_code, state, programme_of_activities, poa_project_id, poa_project_sustaincert_id, poa_project_name, sustainable_development_goals, labels, hsh) 
FROM '/home/shubhangi.bhatia/Desktop/calyx/main_project_details.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');
```

The scraper also extracts the unique sustainability development goals across all projects and saves to the csv file named 'goals.csv' in the working folder. Use the following command to insert that data into the table sust_goals_details:

```sql
\copy gsf_reg.sust_goals_details(goal_name, issuable_products, goal_id) FROM '/home/shubhangi.bhatia/Desktop/calyx/goals.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');
```

## Normalization

The following SQL command is used to populate the `sust_goal_link` table which created a link of each project to its respective sustainability goals.

```sql
WITH goals_data AS (
    -- Extract the goal names from the JSON array in the sustainable_development_goals column
    SELECT 
        main.hsh AS proj_hash,
        jsonb_array_elements(main.sustainable_development_goals::jsonb)->>'name' AS goal_name
    FROM 
        gsf_reg.main_proj_details AS main
)
INSERT INTO gsf_reg.sust_goal_link (proj_hash, goal_id)
SELECT 
    goals_data.proj_hash, 
    goals_details.goal_id
FROM 
    goals_data
JOIN 
    gsf_reg.sust_goals_details AS goals_details
    ON goals_data.goal_name = goals_details.goal_name;
```

## Project Description

The details related to project descriptions are stored in this table. It can be populated with the following command:

```sql
INSERT INTO gsf_reg.proj_desc (
    description,
    project_developer,
    annual_est_credits,
    project_scale,
    methodology,
    credit_period_start_date,
    credit_period_end_date,
    proj_hash
)
SELECT
    description,
    project_developer,
    estimated_annual_credits,
    size AS project_scale,
    methodology,
    crediting_period_start_date AS credit_period_start_date,
    crediting_period_end_date AS credit_period_end_date,
    hsh AS proj_hash
FROM
    gsf_reg.main_proj_details
ON CONFLICT (proj_hash) DO NOTHING; -- Avoid duplicates in case of conflict
```
