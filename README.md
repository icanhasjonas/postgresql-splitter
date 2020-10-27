# postgresql-splitter

Split large postgresql .sql dump files into schema and data files very efficiently.

Build and run with a single argument, which should be your postgresql dump file.
The app will start filtering out the schema part into `_schema.sql` and then extract all table data into `{table-name}.sql`
