# NBA Analytics with dbt 🏀📊

This repository contains a complete **data modeling and analytics layer for NBA statistics** built with **dbt (Data Build Tool)**.  

The project organizes raw NBA data into a **star schema** consisting of:  
- **Dimension models (DIM)**: players, teams, and seasons with enriched metadata.  
- **Fact models (FACT)**: season-level player statistics with advanced metrics (TS%, eFG%, ORB/DRB%, AST/TOV ratio).  
- **Mart layer (MART)**: denormalized models combining player stats, draft data, team information, and awards.  

---

## 🚀 Key Features
- 🏗️ **Modular dbt structure** with staging, intermediate, dimension, fact, and mart layers.  
- 📝 **Comprehensive schema.yml documentation** with standardized `docs` blocks for all columns.  
- 📊 **Advanced basketball analytics**: shooting efficiency, rebounding splits, assist-to-turnover ratios, and more.  
- 🔄 **Normalized & denormalized versions** of datasets for flexibility in reporting.  
- 🧩 Designed for **analytics-ready datasets** that can be used in BI dashboards or machine learning models.  

---

## Project structure

├── models/
│   ├── staging/                  # Raw data cleaning & normalization
│   ├── intermediate/              # Join + transform staging data
│   ├── marts/                     # Dimension, fact, and mart models
│   │   ├── dim/                   # Player, team, season dimensions
│   │   ├── fact/                  # Fact tables with star schema links
│   │   └── mart/                  # Denormalized models for BI
│   └── schema.yml                 # Column-level documentation
├── docs/                          # dbt docs blocks
└── README.md

## Advanced metrics

- The fact and mart layers include calculated advanced basketball metrics:
- **TS% (True Shooting Percentage)** – overall scoring efficiency.
- **eFG% (Effective Field Goal Percentage)** – adjusts FG% for 3PT shots.
- **TOV% (Turnover Percentage)** – possessions ending in turnovers.
- **FTR (Free Throw Rate)** – free throw attempts per field goal attempt.
- **3PAr (Three-Point Attempt Rate)** – three-point attempts per FG attempt.
- **ORB% / DRB%** – offensive and defensive rebound shares.
- **AST/TOV ratio** – playmaking efficiency.


Try running the following commands:
- dbt run
- dbt test