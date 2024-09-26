# PSQL Commands to Store and Normalize Data

This readme contains PostgreSQL (PSQL) commands used for creating schemas, tables, and managing data insertion and normalization.

## Table of Contents
1. [Schema Creation](#schema-creation)
2. [Table Definitions](#table-definitions)
   - [Main Project Details](#main-project-details)
   - [Project Details](#project-details)
   - [Project Description](#project-description)
   - [Sustainable Development Goals](#sustainable-development-goals)
   - [Issuable Products](#issuable-products)
3. [Data Insertion](#data-insertion)
4. [Normalization](#normalization)

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
Project Details

```sql
CREATE TABLE gsf_reg.proj_details (
    gs_id       CHARACTER VARYING NOT NULL,
    proj_name   CHARACTER VARYING,
    proj_status CHARACTER VARYING,
    proj_type   CHARACTER VARYING,
    country     CHARACTER VARYING,
    hsh         CHARACTER VARYING,
    
    -- Define the primary key
    CONSTRAINT proj_details_pkey PRIMARY KEY (gs_id),
    
    -- Define the unique constraint
    CONSTRAINT proj_details_hsh_key UNIQUE (hsh)
);
```
Sustainable Development Goals

```sql
CREATE TABLE gsf_reg.sust_goals (
    name      CHARACTER VARYING,
    proj_hash CHARACTER VARYING,
    issue_id  INTEGER
);
```
Issuable Products

```sql
CREATE TABLE gsf_reg.sust_goals (
    issue_id  INTEGER,
    name      CHARACTER VARYING,
    abbr      CHARACTER VARYING,
    proj_hash CHARACTER VARYING
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
FROM '/home/shubhangi.bhatia/Desktop/calyx/main_proj.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');
```

## Normalization

The following SQL command is used to populate the `proj_details` table with a subset of necessary details from the `main_proj_details` table:

```sql
INSERT INTO gsf_reg.proj_details (gs_id, proj_name, proj_status, proj_type, country, hsh)
SELECT
    CONCAT('GS', sustaincert_id) AS gs_id,
    name AS proj_name,
    status AS proj_status,
    type AS proj_type,
    country,
    hsh
FROM
    gsf_reg.main_proj_details
WHERE
    hsh IS NOT NULL;
```

### Inserting Sustainable Development Goals

The goals from the sustainable_development_goals column in the main_proj_details table will be inserted into the sust_goals table, and each goal will have a unique sequential issue_id and proj_hash that links it back to the main table.

Sustainable Development Goals

```sql
WITH goals AS (
    SELECT
        goal->>'name' AS name,
        main.hsh AS proj_hash  -- Ensure we're getting hsh from the main_proj_details
    FROM
        gsf_reg.main_proj_details AS main,
        jsonb_array_elements(main.sustainable_development_goals::jsonb) AS goal
),
numbered_goals AS (
    SELECT
        name,
        proj_hash,
        ROW_NUMBER() OVER (PARTITION BY proj_hash ORDER BY name) AS issue_id
    FROM
        goals
)
INSERT INTO gsf_reg.sust_goals (name, proj_hash, issue_id)
SELECT
    name,
    proj_hash,
    issue_id
FROM
    numbered_goals;
```
Issuable Products </br> After inserting the goals, we need to store the corresponding issuable products in the sust_issuable_products table.

```sql
WITH json_data AS (
    SELECT
        m/br>ain_proj.hsh AS proj_hash,
        goal->>'name' AS goal_name,
        jsonb_array_elements(goal->'issuable_products') AS product
    FROM
        gsf_reg.main_proj_details main_proj,
        jsonb_array_elements(main_proj.sustainable_development_goals::jsonb) AS goal
)
INSERT INTO gsf_reg.sust_issuable_products (issue_id, name, abbr, proj_hash)
SELECT
    sust.issue_id,
    json_data.product->>'name' AS name,
    json_data.product->>'abbreviation' AS abbr,
    json_data.proj_hash
FROM
    json_data
JOIN
    gsf_reg.sust_goals sust
ON
    json_data.proj_hash = sust.proj_hash
    AND json_data.goal_name = sust.name
WHERE
    json_data.product IS NOT NULL
    AND json_data.product->>'name' IS NOT NULL;
```

Project Description </br> The details related to project descriptions are stored in this table. It can be populated with the following command:

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
