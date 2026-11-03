# Mutation test of the verifier

Source run: `evals/results/v2_haiku_test_r1.json`; 74 verified objects; 189 mutations applied (each a known semantic bug, injected one at a time into a verified conversion).

A mutation the verifier lets through is checked once more: the mutated SQL and the unmutated SQL are run on the same Snowflake data and their outputs compared strictly (exact values, exact row order when the query orders). Identical outputs mean the bug does not show on this data (e.g. `NULLS FIRST` dropped where the sort key has no NULLs, `DISTINCT` dropped where the values are already distinct): no verifier could catch it with this data, so it is reported as *equivalent here*, not as a miss. Different outputs that still passed are real misses of the verifier's comparison rules.

| Mutation | Applied | Caught | Equivalent here | Missed | Catch rate (non-equivalent) |
|---|---|---|---|---|---|
| drop_nulls_first | 55 | 16 | 39 | 0 | 100% |
| try_cast_to_cast | 5 | 1 | 4 | 0 | 100% |
| between_off_by_one | 21 | 10 | 11 | 0 | 100% |
| coalesce_zero_dropped | 1 | 0 | 1 | 0 | - |
| drop_distinct | 8 | 4 | 4 | 0 | 100% |
| round_scale | 7 | 4 | 0 | 3 | 57% |
| flatten_type | 4 | 3 | 0 | 1 | 75% |
| sum_to_avg | 48 | 40 | 8 | 0 | 100% |
| inner_to_left | 40 | 37 | 3 | 0 | 100% |

**Overall: 115 of 119 mutations that change the result were caught (97%)**; 70 more did not change the result on this data; raw catch rate 115 of 189 (61%).

Missed (output changed, verifier passed): B24:flatten_type, A_R01:round_scale, A_R07:round_scale, A_R09:round_scale

Equivalent on this data: A_T01:inner_to_left, B01:drop_nulls_first, B01:between_off_by_one, B03:drop_nulls_first, B05:drop_nulls_first, B05:between_off_by_one, B07:drop_nulls_first, B07:between_off_by_one, B12:drop_nulls_first, B14:drop_distinct, B14:sum_to_avg, B15:drop_nulls_first, B15:between_off_by_one, B17:drop_nulls_first, B18:drop_nulls_first, B24:drop_nulls_first, B26:drop_nulls_first, B28:drop_nulls_first, B30:drop_nulls_first, B32:between_off_by_one, B34:drop_nulls_first, B34:inner_to_left, B36:drop_nulls_first, B36:inner_to_left, B37:drop_nulls_first, B37:between_off_by_one, B38:drop_nulls_first, B38:between_off_by_one, B57:drop_nulls_first, B57:between_off_by_one, Q02:drop_nulls_first, Q04:drop_nulls_first, Q04:sum_to_avg, Q05:try_cast_to_cast, Q09:between_off_by_one, Q16:drop_distinct, Q16:sum_to_avg, Q17:drop_nulls_first, Q20:drop_nulls_first, Q20:try_cast_to_cast, Q22:drop_nulls_first, Q36:drop_nulls_first, Q45:sum_to_avg, Q49:drop_nulls_first, Q49:coalesce_zero_dropped, Q49:sum_to_avg, Q51:drop_nulls_first, Q51:between_off_by_one, Q57:drop_nulls_first, Q62:drop_nulls_first, Q66:drop_nulls_first, Q66:between_off_by_one, Q71:drop_nulls_first, Q76:drop_nulls_first, Q84:drop_nulls_first, Q86:drop_nulls_first, Q94:try_cast_to_cast, Q94:drop_distinct, Q94:sum_to_avg, Q95:try_cast_to_cast, Q95:drop_distinct, Q95:sum_to_avg, Q98:drop_nulls_first, Q99:drop_nulls_first, A_T07:drop_nulls_first, A_R01:drop_nulls_first, A_R06:drop_nulls_first, A_R07:drop_nulls_first, A_R07:sum_to_avg, A_R14:drop_nulls_first
