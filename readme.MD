# PSQL Commands to Store and Normalize Data

This repository contains PostgreSQL (PSQL) commands used for creating schemas, tables, and managing data insertion and normalization.

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

After e

