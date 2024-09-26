# PSQL Commands

This readme contains PostgreSQL (PSQL) commands used for creating schemas, tables, and managing data insertion and normalization.

## Table of Contents
1. [Schema Creation](#schema-creation)
2. [Table Definitions](#table-definitions)
   - [Main Project Details](#main-project-details)
   - [Sustainable Development Goals](#sustainable-development-goals)
   - [Link Table](#link-table)
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
    poa_project_sustaincert_id    NUMERIC,
    poa_project_name              CHARACTER VARYING,
    sustainable_development_goals JSONB,
    labels                        JSONB,
    hsh                           CHARACTER VARYING PRIMARY KEY, -- hsh as primary key
    attached_certificate_files    JSONB
);
```

Sustainable Development Goals

```sql
CREATE TABLE gsf_reg.sust_goals_details (
    goal_name        VARCHAR NOT NULL, -- goal_name as NOT NULL
    issuable_products JSONB,
    goal_id          INTEGER NOT NULL, -- goal_id as NOT NULL
    CONSTRAINT pk_goal_id PRIMARY KEY (goal_id) -- goal_id as the unique primary key
);

```
Link Table
```sql
CREATE TABLE gsf_reg.sust_goal_link (
    proj_hash VARCHAR NOT NULL, -- proj_hash set to NOT NULL
    goal_id   INT NOT NULL,     -- goal_id set to NOT NULL,

    -- Define the composite primary key
    CONSTRAINT pk_sust_goal_link PRIMARY KEY (proj_hash, goal_id),

    -- Foreign key constraint for proj_hash
    CONSTRAINT fk_proj_hash FOREIGN KEY (proj_hash)
        REFERENCES gsf_reg.main_proj_details(hsh)
        ON DELETE CASCADE,

    -- Foreign key constraint for goal_id
    CONSTRAINT fk_goal_id FOREIGN KEY (goal_id)
        REFERENCES gsf_reg.sust_goals_details(goal_id)
        ON DELETE CASCADE

```

## Data Insertion

After extracting the project details into main_project.csv, use the following command to insert the data into the main_proj_details table:

```sql
\copy gsf_reg.main_proj_details(id, created_at, updated_at, name, description, status, gsf_standards_version, estimated_annual_credits, crediting_period_start_date, crediting_period_end_date, methodology, type, size, sustaincert_id, sustaincert_url, project_developer, carbon_stream, country, country_code, state, programme_of_activities, poa_project_id, poa_project_sustaincert_id, poa_project_name, sustainable_development_goals, labels, hsh, attached_certificate_files) 
FROM '/home/shubhangi.bhatia/Desktop/calyx/main_project_details.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');
```

The scraper also extracts the unique Sustainable Development Goals across all projects and saves them to a CSV file named goals.csv in the working folder. Use the following command to insert that data into the sust_goals_details table:

```sql
\copy gsf_reg.sust_goals_details(goal_name, issuable_products, goal_id) FROM '/home/shubhangi.bhatia/Desktop/calyx/goals.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');
```

## Normalization

The following SQL command is used to populate the sust_goal_link table, creating a link between each project and its respective sustainability goals:

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

## Project Description View

The project description details are stored in a view. It can be created with the following command:

```sql
CREATE VIEW gsf_reg.proj_desc AS
SELECT
    sustaincert_id AS gs_id,
    name,
    country AS region,
    status,
    project_developer,
    methodology,
    crediting_period_start_date AS credit_period_start_date,
    crediting_period_end_date AS credit_period_end_date,
    gsf_standards_version AS standards_version,
    estimated_annual_credits AS annual_est_credits,
    size AS project_scale,
    type AS project_type,
    description,
    hsh AS proj_hash
FROM
    gsf_reg.main_proj_details;
```

## Notes

The view can further be indexed on parameters like region, project_scale, etc., if performance optimization is required (e.g., through the use of a materialized view).
