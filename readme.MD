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
