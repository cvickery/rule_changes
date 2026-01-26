#! /usr/local/bin/python3
"""Analyze the course_rule_matching_verification.csv file."""

import csv
import re
import sys
from collections import namedtuple, defaultdict

multi_valence = defaultdict(list)

def course_counts(course: str):
  return {
    'course': course,
    'POSITIVE':{'course_counted': 0, 'new_rules': 0, 'num_before': 0, 'num_after': 0},
    'NEUTRAL': {'course_counted': 0, 'new_rules': 0, 'num_before': 0, 'num_after': 0},
    'NEGATIVE': {'course_counted': 0, 'new_rules': 0, 'num_before': 0, 'num_after': 0}
    }


def update_global(this_course: dict):
  valence_analysis['total_courses'] += 1
  valences_counted = []
  for valence in ['POSITIVE', 'NEUTRAL', 'NEGATIVE']:
    if this_course[valence]['course_counted']:
      valences_counted.append(valence)
      valence_analysis[valence]['num_courses'] += this_course[valence]['course_counted']
      valence_analysis[valence]['new_rules'] += this_course[valence]['new_rules']
      valence_analysis[valence]['total_before'] += this_course[valence]['num_before']
      valence_analysis[valence]['total_after'] += this_course[valence]['num_after']

      if this_course[valence]['num_after'] > this_course[valence]['num_before']:
        valence_analysis[valence]['num_more'] += 1
      elif this_course[valence]['num_after'] < this_course[valence]['num_before']:
        valence_analysis[valence]['num_less'] += 1
      else:
        valence_analysis[valence]['num_same'] += 1

  if len(valences_counted) > 1:
    multi_valence[str(valences_counted)].append(this_course['course'])



def info_dict():
  return {
    'num_cases': 0,
    'num_courses': 0,
    'new_rules': 0,
    'total_before': 0,
    'total_after': 0,
    'num_more': 0,
    'num_less': 0,
    'num_same': 0
  }


if __name__ == '__main__':

  with open('course-rules.csv') as csv_file:
    valence_analysis = {
      'total_courses': 0,
      'POSITIVE': info_dict(),
      'NEUTRAL': info_dict(),
      'NEGATIVE': info_dict()
    }
    Row = None
    reader = csv.reader(csv_file)
    for row in reader:
      if not Row:
        cols = [c.lower().replace(' ', '_') for c in row]
        Row = namedtuple('Row', cols)
        this_course = course_counts('')  # blank course is first-row flag
      else:
        try:
          row = Row._make(row)
        except TypeError:

          # End of data: pick up the last course
          update_global(this_course)

          # Display analyses and exit
          print('MULTI-VALENCE COURSES', file=sys.stderr)
          total_multi = 0
          for key, value in multi_valence.items():
            total_multi += len(value)
            print(f'  {key}: {', '.join(value)}', file=sys.stderr)
          s = '' if total_multi == 1 else 's'
          print(f'{total_multi:,} total multi-valence course{s}', file=sys.stderr)
          print(file=sys.stderr)
          print(f'TOTAL COURSES: {valence_analysis['total_courses']:7,}', file=sys.stderr)
          print(file=sys.stderr)
          for valence in ['POSITIVE', 'NEUTRAL', 'NEGATIVE']:
            print(f'{valence}:', file=sys.stderr)
            for key, value in valence_analysis[valence].items():
              print(f'  {key:>12} {value:>5,}', file=sys.stderr)
          exit()

        # Global record count
        valence_analysis[row.valence]['num_cases'] += 1

        # Manage course changes
        if row.sending_course != this_course['course']:
          if this_course['course']:
            # Update global stats for prior course
            update_global(this_course)
          this_course = course_counts(row.sending_course)

        valence = row.valence
        this_course[valence]['course_counted'] = 1
        if not row.old_description:
          this_course[valence]['new_rules'] += 1

        # There can be multiple destination courses, so use re.findall to find them all
        if row.old_description:
          this_course[valence]['num_before'] += sum([int(x) for x in re.findall(r"\[.*?:(\d+)\]",
            row.old_description.split('=>')[1])])
        if row.new_description:
          this_course[valence]['num_after'] += sum([int(x) for x in re.findall(r"\[.*?:(\d+)\]",
            row.new_description.split('=>')[1])])

        print(f'{row.sending_course:10} {row.valence:8} {this_course}')
