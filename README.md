Case Study Upstart13

I built the whole pipeline in PostgreSQL using a medallion approach with 3 schemas:

- **raw** - CSVs loaded as-is,all TEXT,no transformations
- **store** - types enforced,data cleaned,PKs and FKs declared
- **publish** - business rules applied,joins and calculated fields

Files

`01-extract.sql` - I created the 3 schemas,loaded the CSVs into raw, and transforms into store with proper types.

`02-transform_load.sql` - I applied the business rules (for color,category,LeadTime,TotalLineExtendedPrice),built the publish tables,and ran the two question queries at the end.

Results

Highest revenue color by year:

| Year | Color | Revenue |
|------|-------|---------|
| 2021 | Red | 6,019,614.02 |
| 2022 | Black | 14,005,242.98 |
| 2023 | Black | 15,047,694.37 |
| 2024 | Yellow | 6,368,158.48 |

Avg LeadTimeInBusinessDays by category:

| Category | Avg Lead Time |
|----------|---------------|
| Others | 5.72 |
| Clothing | 5.71 |
| Accessories | 5.70 |
| Bikes | 5.67 |
| Components | 5.67 |
