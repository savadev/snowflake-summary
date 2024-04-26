-- Multi-Cluster warehouses are disabled with trial account (and standard accounts)
 
CREATE OR REPLACE WAREHOUSE ANALYST_WH with
warehouse_size='SMALL'
auto_suspend = 60
auto_resume = true
initially_suspended=true;

CREATE OR REPLACE WAREHOUSE AUDIENCE_WH with
warehouse_size='SMALL'
--min_cluster_count=1
--max_cluster_count=6
auto_suspend = 60
auto_resume = true
initially_suspended=true;

CREATE OR REPLACE WAREHOUSE DASHBOARD_WH with
warehouse_size='MEDIUM'
--min_cluster_count=1
--max_cluster_count=1
auto_suspend = 60
auto_resume = true
initially_suspended=true;

CREATE OR REPLACE WAREHOUSE DATALOADER with
warehouse_size='LARGE'
--min_cluster_count=1
--max_cluster_count=2
auto_suspend = 60
auto_resume = true
initially_suspended=true;

CREATE OR REPLACE WAREHOUSE DATALOADER_2 with
warehouse_size='LARGE'
--min_cluster_count=1
--max_cluster_count=2
auto_suspend = 60
auto_resume = true
initially_suspended=true;

CREATE OR REPLACE WAREHOUSE ENRICHMENT_WH with
warehouse_size='SMALL'
--min_cluster_count=1
--max_cluster_count=3
auto_suspend = 60
auto_resume = true
initially_suspended=true;

CREATE OR REPLACE WAREHOUSE KEYWORD_WH with
warehouse_size='SMALL'
--min_cluster_count=1
--max_cluster_count=3
auto_suspend = 60
auto_resume = true
initially_suspended=true;

CREATE OR REPLACE WAREHOUSE LORENZO_TEST with
warehouse_size='XSMALL'
--min_cluster_count=1
--max_cluster_count=1
auto_suspend = 60
auto_resume = true
initially_suspended=true;

CREATE OR REPLACE WAREHOUSE PIXEL_WH with
warehouse_size='XSMALL'
--min_cluster_count=1
--max_cluster_count=5
auto_suspend = 60
auto_resume = true
initially_suspended=true;

