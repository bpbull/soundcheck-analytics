Soundcheck — Analytics Engineering Portfolio

**Stack:** BigQuery • dbt Core • Looker Studio • Python  
**Skills Demonstrated:** Data Modeling • ETL Pipelines • Data Quality Testing • Dashboard Development

> **Goal**: Showcase end-to-end analytics engineering skills through a realistic music industry dataset, from raw data generation to business intelligence dashboards.

## Project Overview

This project simulates a comprehensive analytics pipeline for a proof-of-concept live music platform, demonstrating:

- **Data Engineering**: Python-generated synthetic dataset with realistic business logic
- **Cloud Data Warehousing**: BigQuery implementation with proper partitioning and optimization  
- **Data Modeling**: dbt transformations following dimensional modeling principles
- **Data Quality**: Comprehensive testing and data validation
- **Business Intelligence**: Executive dashboards with actionable insights

## Business Context

Soundcheck tracks:
- **Artist Performance**: Tours, venue relationships, ticket sales
- **Venue Operations**: Capacity utilization, revenue optimization
- **Fan Engagement**: Purchase patterns, review sentiment, loyalty metrics
- **Market Analysis**: Geographic trends, seasonal patterns, pricing strategies

## Technical Architecture

```
Data Generation (Python) → BigQuery (Raw) → dbt (Transform) → Looker Studio (Visualize)
```

### Repository Structure
```
generator/        # Synthetic data generation with business logic
data/raw/         # Local CSV staging (not committed)
bq/               # BigQuery schemas, partitioning, and load scripts  
dbt/              # Data transformations, tests, and documentation
dashboards/       # BI dashboard links and screenshots
```

## Key Features

### Data Generation (`generator/`)
- **Realistic Relationships**: Artists have tours, venues have capacity constraints
- **Seasonality**: Concert patterns reflect real-world touring seasons
- **Business Logic**: Ticket pricing based on demand, venue size, artist popularity

### Data Warehouse (`bq/`)
- **Partitioned Tables**: Events partitioned by date for query performance
- **Proper Schemas**: Separate datasets for raw, staging, and mart layers
- **Cost Optimization**: Clustered tables and efficient data types

### Data Transformations (`dbt/`)
- **Dimensional Modeling**: Star schema with fact and dimension tables
- **Data Quality Tests**: Referential integrity, uniqueness, and freshness checks
- **Documentation**: Auto-generated data lineage and field descriptions

### Analytics (`dashboards/`)
- **Executive KPIs**: Revenue trends, capacity utilization, customer metrics
- **Operational Insights**: Venue performance, artist rankings, geographic analysis
- **Drill-Down Capability**: From high-level metrics to transaction details

## Getting Started

### Prerequisites
- Google Cloud Platform account with BigQuery enabled
- dbt Core installed locally
- Python for data generation

### Quick Start

**Current Status**: Data generation complete, BigQuery and dbt setup in progress

1. **Generate Data**:
```bash
   cd generator/
   python generate_fake_data.py
```

2. **Setup BigQuery (in progress)**:
   Load scripts and schema definitions in development

3. **dbt Development In progress (in progress)**:
```bash
   cd dbt/
   # Coming soon: dbt deps && dbt run && dbt test
```

4. **View Dashboard**: [Looker Studio Link](coming soon)

## Sample Insights

This project answers business questions like:
- Which venues have the highest revenue per square foot?
- What's the optimal pricing strategy by artist tier and market?
- How does day-of-week affect ticket sales conversion?
- Where are we consistently underpricing tickets based on demand elasticity and sell-out velocity?
- Which artists are rising fastest in popularity based on follow rates and ticket sales momentum?
- Which events generate the highest revenue per attendee, factoring in ticket tiers and upsells?

## Data Sample

Since raw CSV files are large (500MB+), sample data is available:
- **Development**: Sample data files coming soon to `dbt/seeds/`
- **Full Dataset**: Available via [Google Cloud Storage](coming soon)

## Project Status

- [x] Synthetic data generation with business logic
- [ ] BigQuery schema design and data loading
- [ ] dbt staging and mart layer development  
- [ ] Comprehensive data quality testing
- [ ] Executive dashboard development
- [ ] Performance optimization and documentation


**Technical Decisions Made**:
- Chose BigQuery for its columnar storage and built-in ML capabilities
- Used dbt for version-controlled, testable transformations
- Implemented star schema for optimal query performance
- Added data quality tests to ensure reliability

**Challenges Solved**:
- Handling many-to-many relationships (artists-venues through events)
- Implementing realistic business constraints in synthetic data
- Balancing data volume with query performance
- Creating meaningful KPIs for diverse stakeholders

**Next Steps**:
- Implement incremental models for large fact tables
- Add machine learning models for demand forecasting
- Create real-time streaming pipeline with Pub/Sub
- Expand to include social media sentiment analysis

## Contact

Brendan Bullivant - [bpbullivant3@gmail.com]  
LinkedIn: [www.linkedin.com/in/brendan-bullivant]  

---

*This project demonstrates modern analytics engineering practices and is available for discussion during technical interviews.*
