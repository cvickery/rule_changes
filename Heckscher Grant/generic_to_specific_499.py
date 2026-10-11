#! /usr/bin/env python3
"""Valence counts for rules that changed 499s from generic to discipline-specific."""

import csv
import re
import sys

from collections import namedtuple

counts = {
  'POSITIVE': {'count': 0, 'potential': 0, 'disciplines': set()},
  'NEUTRAL': {'count': 0, 'potential': 0, 'disciplines': set()},
  'NEGATIVE': {'count': 0, 'potential': 0, 'disciplines': set()},
}

with open('course-rules.csv') as f:
  generic_to_discipline = 0
  generic_to_generic = 0
  reader = csv.reader(f)
  Row = None
  for line in reader:
    if not Row:
      cols = [c.lower().replace(' ', '_') for c in line]
      Row = namedtuple('Row', cols)
    else:
      try:
        row = Row._make(line)
        if row.old_description and row.new_description:
          old_send, old_receive = row.old_description.split('=>')
          new_send, new_receive = row.new_description.split('=>')
          # Is old destination LAE or NLAE 499s and no other course?
          if 'and' in old_receive:
            continue
          if re.search(r'LAE|NLAE', old_receive):
            # Is new destination a single 499 course?
            if 'and' in new_receive:
              continue
            try:
              discipline = re.search(r'([A-Z]+) 499', new_receive)[1]
              if discipline in ['LAE', 'NLAE']:
                generic_to_generic += 1
              else:
                generic_to_discipline += 1
                counts[row.valence]['count'] += 1
                counts[row.valence]['disciplines'].add(discipline)
                before = sum([int(x) for x in re.findall(r"\[.*?:(\d+)\]",
                            row.old_description.split('=>')[1])])
                after = sum([int(x) for x in re.findall(r"\[.*?:(\d+)\]",
                            row.new_description.split('=>')[1])])
                if after == 0 or after <= before:
                  counts[row.valence]['potential'] += 1
            except TypeError:
              continue

      except TypeError:
        if generic_to_generic != 0:
          print(f'{generic_to_generic:,} generic electives did not change.', file=sys.stderr)
        print(f'{generic_to_discipline:,} generic electives changed to discipline-specific.',
              file=sys.stderr)
        print(f'{len(counts['POSITIVE']['disciplines'].union(counts['NEUTRAL']['disciplines'])):>3} '
              f'specific disciplines', file=sys.stderr)
        print('  Valence   Count Potential Discipline(s)', file=sys.stderr)
        for valence, values in counts.items():
          print(f'{valence:>9} {values['count']:>7,} {values['potential']:>9,} '
                f'{', '.join(sorted(values['disciplines']))}',
                file=sys.stderr)
        exit()
