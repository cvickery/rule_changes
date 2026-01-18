copy
(select c.discipline||' '||c.catalog_number as course, c.designation as rd,
       c.course_status as stat, d.rule_key, d.description
from cuny_courses c, rule_descriptions d, transfer_rules r, source_courses s
where c.institution = 'QCC01'
  and c.discipline||' '||c.catalog_number in ('CIS 101', 'DAN 101', 'DAN 126', 'DAN 136', 'DAN 230', 'HE 102', 'PH 412', 'PH 413')
  and c.course_id = s.course_id
  and c.offer_nbr = s.offer_nbr
  and r.id = s.rule_id
  and d.rule_key = r.rule_key
  and d.rule_key ~* 'qns'
order by course)

to '/Users/vickery/Projects/rule_changes/Heckscher Grant/missing_rules.csv' csv header
