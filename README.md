## Solution Accelarator for handling Sensitive data on the Databricks Data Intelligence Platform

#### For a detailed conceptual understanding of Handling Secntitive data on the Databricks Data Intelligence Platform, please refer to the following blogpost
[Handling Sensitive Data with Databricks Data Intelligence Platform]()

#### Requirements:

1. Unity Catalog enabled Databricks Workspaces
2. User with Workspace Admin Privilege
3. Unity Catalog  enabled cluster with DBR 13.3 or above


#### Contents

The solution accelarator includes sample scripts for the following tasks

1. Generate fake PII data using Python faker library
2. PII detection and tagging in Unity Catalog governed Tables using Databricks labs project [DiscoverX](https://github.com/databrickslabs/discoverx) and [Presidio](https://github.com/microsoft/presidio)
3. Encrypt columns (including free text columns)
4. Apply dynamic column level masking

#### How to run

1. Clone the repository into Databricks Workspace Repo

2. Create UC Catalogs and Schemas (examples below)
  - DIZ Catalog: example
  - DIZ Schema: default
  - Dev Catalog: example_dev
  - Dev Schema: default
  - Dev Catalog: example_prod
  - Dev Schema: default

3. Create privileged users account group (example: prod-privileged-users) and add the users. This is used for fine grained access controls.

4. Run the notebooks

    Option 1: Run the notebooks manually in sequence.
      - Run all the notebooks (except notebook 4. Prod CLM enforcement.py) using a Cluster in Assigned mode (Single User). DBR 13.3 or above
  
    Option2: Create a workflow and create a Task for each Notebook
      - Set the following job parameters

      | Name    | Value |
      | -------- | ------- |
      | diz_catalog  | example    |
      | diz_schema | default     |
      | num_rows    | 1000    |
      | prod_catalog    | example_prod    |
      | dev_catalog    | example_dev    |
      | target_schema    | default    |
      | free_text    | freetext    |
      | privileged_group_name    | prod-privileged-users    |

5. Observe the results: Browse the bronze and silver tables in the specified catalog/schema.


