#! /usr/local/bin/python3

import csv

from collections import namedtuple

counts = {
  'POSITIVE': {'added': 0, 'changed': 0, 'dropped': 0},
  'NEUTRAL': {'added': 0, 'changed': 0, 'dropped': 0},
  'NEGATIVE': {'added': 0, 'changed': 0, 'dropped': 0},
}

with open('course-rules.csv') as f:
  reader = csv.reader(f)
  Row = None
  for line in reader:
    if not Row:
      cols = [c.lower().replace(' ', '_') for c in line]
      Row = namedtuple('Row', cols)
    else:
      try:
        row = Row._make(line)
        if not row.old_description:
          key = 'added'
        elif not row.new_description:
          key = 'dropped'
        else:
          key = 'changed'
        counts[row.valence][key] += 1
      except TypeError:
        print('  Valence   Added Changed Dropped')
        for valence, values in counts.items():
          print(f'{valence:>9} {values['added']:>7,} {values['changed']:>7,} {values['dropped']:>7,}')
        exit()
