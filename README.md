# Repository Coverage



| Name                                                                   |    Stmts |     Miss |   Cover |   Missing |
|----------------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| versioned\_collection/\_\_init\_\_.py                                  |        3 |        0 |    100% |           |
| versioned\_collection/collection/\_\_init\_\_.py                       |        2 |        0 |    100% |           |
| versioned\_collection/collection/tracking\_collections/\_\_init\_\_.py |       11 |        0 |    100% |           |
| versioned\_collection/collection/tracking\_collections/base.py         |       23 |        1 |     96% |        40 |
| versioned\_collection/collection/tracking\_collections/branches.py     |       70 |       18 |     74% |40-44, 62, 93, 118, 160-170, 189 |
| versioned\_collection/collection/tracking\_collections/conflicts.py    |       16 |        0 |    100% |           |
| versioned\_collection/collection/tracking\_collections/deltas.py       |      283 |       60 |     79% |72, 88, 112, 200, 230-311, 332-339, 342, 347-349, 426, 454, 623-624, 721-729, 739, 755 |
| versioned\_collection/collection/tracking\_collections/lock.py         |       33 |       10 |     70% |51, 61-65, 73-77, 85-89 |
| versioned\_collection/collection/tracking\_collections/logs.py         |      268 |       45 |     83% |73, 75, 77, 95, 97, 99, 153-154, 165-171, 188, 217, 238, 257-269, 300, 306, 312, 488, 491-497, 507, 533, 547-552, 574-575, 624 |
| versioned\_collection/collection/tracking\_collections/metadata.py     |       49 |        1 |     98% |        86 |
| versioned\_collection/collection/tracking\_collections/modified.py     |       29 |        1 |     97% |        58 |
| versioned\_collection/collection/tracking\_collections/replica.py      |       10 |        0 |    100% |           |
| versioned\_collection/collection/tracking\_collections/stash.py        |       39 |        0 |    100% |           |
| versioned\_collection/collection/versioned\_collection.py              |      847 |       49 |     94% |217, 418, 449, 462-467, 471-472, 1299-1300, 1309, 1414, 2019, 2028-2032, 2081, 2091, 2098, 2169, 2176, 2195, 2232, 2282, 2296-2331, 2441 |
| versioned\_collection/errors.py                                        |       20 |        0 |    100% |           |
| versioned\_collection/listener.py                                      |       94 |       19 |     80% |89, 113, 134, 202-209, 214, 218-229, 235 |
| versioned\_collection/tree.py                                          |       85 |       12 |     86% |26, 28, 74, 98-99, 113, 152-157, 162 |
| versioned\_collection/utils/\_\_init\_\_.py                            |        0 |        0 |    100% |           |
| versioned\_collection/utils/data\_structures.py                        |        1 |        0 |    100% |           |
| versioned\_collection/utils/events.py                                  |       45 |        0 |    100% |           |
| versioned\_collection/utils/mongo\_query.py                            |       27 |        7 |     74% | 47, 58-73 |
| versioned\_collection/utils/multi\_processing.py                       |       12 |        1 |     92% |        10 |
| versioned\_collection/utils/serialization.py                           |       33 |        8 |     76% |18, 21, 26-31 |
|                                                              **TOTAL** | **2000** |  **232** | **88%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://github.com/HumanisingAutonomy/versioned_collection/raw/ci/coverage_badge/badge.svg)](https://github.com/HumanisingAutonomy/versioned_collection/tree/ci/coverage_badge)

This is the one to use if your repository is private or if you don't want to customize anything.



## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.