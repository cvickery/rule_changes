#! /usr/local/bin/python3
"""Given a pair of dates find the last archive set prior to each date, and generate a CSV file
   showing what CUNY transfer rules changed from one archive date to the next.
"""

import psycopg
import sys

from bisect import bisect_right
from datetime import date
from mk_descriptions import describe_rules
from pathlib import Path
from psycopg.rows import namedtuple_row
from time import time

# main()
# -------------------------------------------------------------------------------------------------
if __name__ == '__main__':

  start_time = time()
  # Get list of available archives
  reports_dir = Path(Path.cwd(), 'reports')
  archive_dir = Path(Path.home(), 'Projects/cuny_curriculum/rules_archive')
  if not archive_dir.is_dir():
    exit('Rules archive dir not found')
  archive_dates = []
  for file_path in archive_dir.glob('*effective*'):
    archive_dates.append(date.fromisoformat(file_path.name[0:10]))
  archive_dates.sort()
  print(f'{len(archive_dates):,} archives from {archive_dates[0]} to {archive_dates[-1]}')

  # Get list of target date strings; convert to dates; sort; generate pairs
  targets = sorted([date.fromisoformat(arg) for arg in sys.argv[1:]])
  for first_target, second_target in list(zip(targets, targets[1:])):

    # Find the last archive at or before both_target dates
    print(f'\nTargets are {first_target} and {second_target}')
    if (first_date_index := bisect_right(archive_dates, first_target) - 1) < 0:
      first_date_index = 0
    first_date = archive_dates[first_date_index]
    first_schema = f'_{str(first_date).replace('-', '_')}'

    if (second_date_index := bisect_right(archive_dates, second_target) - 1) < 0:
      second_date_index = 0
    second_date = archive_dates[second_date_index]
    second_schema = f'_{str(second_date).replace('-', '_')}'

    # Check if the target dates resolve to the same archive dates
    if first_schema == second_schema:
      print(f'Both {first_target} and {second_target} resolve to the same archive ({first_date}). '
            f'Skipping')
      continue

    print(f'Verify archive sets for {first_date} and {second_date}')

    # Be sure all six archive files are available. List all errors before failing.
    fail = False

    first_source_archive = Path(archive_dir, f'{first_date}_source_courses.csv.bz2')
    if not first_source_archive.is_file():
      print(f'{first_source_archive} is not a file')
      fail = True
    first_destination_archive = Path(archive_dir, f'{first_date}_destination_courses.csv.bz2')
    if not first_destination_archive.is_file():
      print(f'{first_destination_archive} is not a file')
      fail = True
    first_effective_dates_archive = Path(archive_dir, f'{first_date}_effective_dates.csv.bz2')
    if not first_effective_dates_archive.is_file():
      print(f'{first_effective_dates_archive} is not a file')
      fail = True

    second_source_archive = Path(archive_dir, f'{second_date}_source_courses.csv.bz2')
    if not second_source_archive.is_file():
      print(f'{second_source_archive} is not a file')
      fail = True
    second_destination_archive = Path(archive_dir, f'{second_date}_destination_courses.csv.bz2')
    if not second_destination_archive.is_file():
      print(f'{second_destination_archive} is not a file')
      fail = True
    second_effective_dates_archive = Path(archive_dir, f'{second_date}_effective_dates.csv.bz2')
    if not second_effective_dates_archive.is_file():
      print(f'{second_effective_dates_archive} is not a file')
      fail = True

    if fail:
      continue

    print(f'{time() - start_time:.1f} sec')

    print(f'Load archive sets into schemata {first_schema} and {second_schema}')
    with psycopg.connect('dbname=cuny_curriculum') as conn:
      with conn.cursor(row_factory=namedtuple_row) as cursor:
        for schema_date, schema_name in ((first_date, first_schema), (second_date, second_schema)):
          # Is the archive set already loaded into the db?
          cursor.execute("""
          select exists (select 1 from information_schema.tables
          where table_schema = %s
            and table_name = %s)""", (schema_name, 'rule_descriptions'))
          schema_ready = cursor.fetchone().exists
          if schema_ready:
            print(f'{schema_name} already loaded')
          else:
            print(f'Load archive set into schema {schema_name}')
            cursor.execute(f"""
            drop schema if exists {schema_name} cascade;
            create schema {schema_name};

            create unlogged table {schema_name}.transfer_rules (
              rule_key       text primary key,
              effective_date date
            );

            create unlogged table {schema_name}.source_courses (
              rule_key     text not null,
              course_id    int,
              offer_nbr    int,
              min_credits  real,
              max_credits  real,
              credit_src   text,
              min_gpa      real,
              max_gpa      real
            );

            create unlogged table {schema_name}.destination_courses (
              rule_key     text not null,
              course_id    int,
              offer_nbr    int,
              credits      real
            );

            create index on {schema_name}.source_courses (rule_key);
            create index on {schema_name}.destination_courses (rule_key);

            copy {schema_name}.transfer_rules
            from program $$bunzip2 -c {archive_dir}/{schema_date}_effective_dates.csv.bz2$$
            csv;

            copy {schema_name}.source_courses
            from program $$bunzip2 -c {archive_dir}/{schema_date}_source_courses.csv.bz2$$
            csv;

            copy {schema_name}.destination_courses
            from program $$bunzip2 -c {archive_dir}/{schema_date}_destination_courses.csv.bz2$$
            csv;

            create index if not exists {schema_name}_source_courses_rule_key_idx
              on {schema_name}.source_courses(rule_key);

            analyze {schema_name}.source_courses;
            analyze {schema_name}.destination_courses;
            analyze {schema_name}.transfer_rules;
            """)
            conn.commit()

            cursor.execute(f"""
            -- Get the length of the transfer_rules table
            select count(*) as num_rules from {schema_name}.transfer_rules;
            """)
            num_rules = cursor.fetchone().num_rules
            print(f'{time() - start_time:.1f} sec')

            # Create and populate the rule_descriptions table
            print(f'Make rule descriptions for {schema_date}')
            rule_descriptions = describe_rules(schema_name)
            print(f'{len(rule_descriptions):,} {schema_date} rules')
            cursor.execute(f"""
              drop table if exists {schema_name}.rule_descriptions;
              create table {schema_name}.rule_descriptions (
                rule_key       text primary key,
                effective_date text,
                description    text
              )
              """)
            with cursor.copy(f'copy {schema_name}.rule_descriptions '
                             f'(rule_key, effective_date, description) from stdin') as cpy:
              for row in rule_descriptions:
                cpy.write_row(row)
            conn.commit()
            print(f'{time() - start_time:.1f} sec')

        # Generate the CSV for this pair
        report_path = Path(reports_dir, f'{first_date}_{second_date}.csv')
        print(f'Generate {report_path}')
        cursor.execute(f"""
        COPY (
          SELECT
            COALESCE(f.rule_key, s.rule_key) AS "Rule Key",
            f.description                    AS "{first_date} Rule",
            f.effective_date                 AS "Effective Date",
            s.description                    AS "{second_date} Rule",
            s.effective_date                 AS "Effective Date"
          FROM {first_schema}.rule_descriptions  AS f
          FULL OUTER JOIN {second_schema}.rule_descriptions AS s
            USING (rule_key)
          WHERE
            -- consider NULL ≠ value and value ≠ value
            f.description IS DISTINCT FROM s.description
          ORDER BY "Rule Key")
        TO '{report_path}' CSV HEADER
        """)
        print(f'{time() - start_time:.1f} sec')
