# -*- coding: utf-8 -*-

import unittest

from collections import namedtuple

from stream import Stream, ParallelStream


def parametrized(*lst):

    def wrapper(func):

        def inner_wrapper(*args, **kwargs):
            for bundle in lst:
                func(*args, *bundle, **kwargs)

        return inner_wrapper

    return wrapper


def repeat(n):

    def wrapper(func):

        def inner_wrapper(*args, **kwargs):
            for i in range(n):
                func(*args, **kwargs)

        return inner_wrapper

    return wrapper


class TestStream(unittest.TestCase):

    def assert_all_true(self, iterable, predicate):
        for element in iterable:
            self.assertTrue(predicate(element))

    @parametrized([Stream], [ParallelStream])
    def test_empty(self, cls):
        self.assertListEqual(
            cls.empty().to_list(),
            []
        )

    @parametrized([Stream], [ParallelStream])
    def test_range(self, cls):
        self.assertListEqual(
            cls.range(0, 10).to_list(),
            [x for x in range(10)]
        )

    @parametrized([Stream], [ParallelStream])
    def test_range_closed(self, cls):
        self.assertListEqual(
            cls.range_closed(0, 9).to_list(),
            [x for x in range(10)]
        )

    def test_concat(self):
        self.assertListEqual(
            Stream.concat(
                Stream.of((0, 1)),
                Stream.of((2, 3)),
                Stream.concat(
                    Stream.of((4, 5)),
                    Stream.of((6, 7)),
                    Stream.concat(
                        Stream.concat(
                            Stream.of((8, 9))
                        )
                    )
                )
            ).to_list(),
            [x for x in range(10)]
        )

    @repeat(10)
    def test_parallel_concat(self):
        self.assertListEqual(
            sorted(
                ParallelStream.concat(
                    ParallelStream.of((0, 1)),
                    ParallelStream.of((2, 3)),
                    ParallelStream.concat(
                        ParallelStream.of((4, 5)),
                        ParallelStream.of((6, 7)),
                        ParallelStream.concat(
                            ParallelStream.concat(
                                ParallelStream.of((8, 9))
                            )
                        )
                    )
                ).to_list()
            ),
            [x for x in range(10)]
        )

    @parametrized([Stream], [ParallelStream])
    def test_random(self, cls):
        self.assert_all_true(
            cls.random(0, 10).limit(1000).to_list(),
            lambda x: 0 <= x < 10
        )

    @parametrized([Stream], [ParallelStream])
    def test_random_closed(self, cls):
        self.assert_all_true(
            cls.random_closed(0, 9).limit(1000).to_list(),
            lambda x: 0 <= x < 10
        )

    @parametrized([Stream], [ParallelStream])
    def test_generate(self, cls):
        self.assertListEqual(
            cls.generate(lambda: 0).limit(3).to_list(),
            [0, 0, 0]
        )

    @parametrized([Stream], [ParallelStream])
    def test_distinct(self, cls):
        self.assertListEqual(
            cls.of((0, 1, 0, 1, 2, 2, 1, 0, 1, 0)).distinct().to_list(),
            [0, 1, 2]
        )

    @parametrized([Stream], [ParallelStream])
    def test_filter(self, cls):
        self.assertListEqual(
            cls.of((0, 1, 0, 1, 0)).filter(lambda x: x == 0).to_list(),
            [0, 0, 0]
        )

    @parametrized([Stream], [ParallelStream])
    def test_flatten(self, cls):
        self.assertListEqual(
            cls.of((0, Stream.of((1, 2)), Stream.of((3, 4)))).flatten().to_list(),
            [0, 1, 2, 3, 4]
        )

    def test_limit(self):
        self.assertListEqual(
            Stream.of((0, 1, 2, 3, 4)).limit(3).to_list(),
            [0, 1, 2]
        )

    @repeat(10)
    def test_parallel_limit(self):
        self.assertEqual(
            len(
                ParallelStream.concat(
                    ParallelStream.random(0, 10),
                    ParallelStream.random(0, 10),
                    ParallelStream.random(0, 10),
                    ParallelStream.random(0, 10),
                    ParallelStream.random(0, 10)
                ).limit(100).to_list()
            ),
            100
        )

    @parametrized([Stream], [ParallelStream])
    def test_map(self, cls):
        self.assertListEqual(
            cls.of((0, 1, 2, 3, 4)).map(lambda x: x + 1).to_list(),
            [1, 2, 3, 4, 5]
        )

    @parametrized([Stream], [ParallelStream])
    def test_peek(self, cls):
        lst = []

        self.assertListEqual(
            cls.of((0, 1, 2, 3, 4)).peek(lambda x: lst.append(x)).to_list(),
            [0, 1, 2, 3, 4]
        )

        self.assertListEqual(
            lst,
            [0, 1, 2, 3, 4]
        )

    @parametrized([Stream], [ParallelStream])
    def test_skip(self, cls):
        self.assertListEqual(
            cls.of((0, 1, 2, 3, 4)).skip(3).to_list(),
            [3, 4]
        )

    @parametrized([Stream], [ParallelStream])
    def test_all_match(self, cls):
        self.assertTrue(
            cls.of((0, 2, 4)).all_match(lambda x: x % 2 == 0)
        )

    @parametrized([Stream], [ParallelStream])
    def test_any_match(self, cls):
        self.assertTrue(
            cls.of((0, 2, 4)).any_match(lambda x: x == 2)
        )

    @parametrized([Stream], [ParallelStream])
    def test_average(self, cls):
        self.assertEqual(
            cls.of((0, 1, 2, 3, 4)).average(),
            2
        )

    @parametrized([Stream], [ParallelStream])
    def test_count(self, cls):
        self.assertEqual(
            cls.of((0, 1, 2, 3, 4)).count(),
            5
        )

    def test_find_any(self):
        self.assertEqual(
            ParallelStream.of((0, 1, 2, 3, 4)).find_any(),
            0
        )

    def test_find_first(self):
        self.assertEqual(
            Stream.of((0, 1, 2, 3, 4)).find_first(),
            0
        )

    def test_find_last(self):
        self.assertEqual(
            Stream.of((0, 1, 2, 3, 4)).find_last(),
            4
        )

    @parametrized([Stream], [ParallelStream])
    def test_for_each(self, cls):
        lst = []

        cls.of((0, 1, 2, 3, 4)).for_each(lambda x: lst.append(x))

        self.assertListEqual(
            lst,
            [0, 1, 2, 3, 4]
        )

    @parametrized([Stream], [ParallelStream])
    def test_group_by(self, cls):
        self.assertDictEqual(
            cls.of((0, 1, 2, 3, 4)).group_by(lambda x: x % 2),
            {0: [0, 2, 4], 1: [1, 3]}
        )

    @parametrized([Stream], [ParallelStream])
    def test_join(self, cls):
        self.assertEqual(
            cls.of(('0', '1', '2', '3', '4')).join('\n'),
            '0\n1\n2\n3\n4'
        )

    @parametrized([Stream], [ParallelStream])
    def test_max(self, cls):
        self.assertEqual(
            cls.of((0, 1, 0, 1, 0)).max(),
            1
        )

    @parametrized([Stream], [ParallelStream])
    def test_min(self, cls):
        self.assertEqual(
            cls.of((1, 0, 1, 0, 1)).min(),
            0
        )

    @parametrized([Stream], [ParallelStream])
    def test_none_match(self, cls):
        self.assertTrue(
            cls.of((0, 2, 4)).none_match(lambda x: x % 2 == 1)
        )

    @parametrized([Stream], [ParallelStream])
    def test_reduce(self, cls):
        self.assertEqual(
            cls.of((0, 1, 2, 3, 4)).reduce(0, lambda x, y: x + y),
            10
        )

    @parametrized([Stream], [ParallelStream])
    def test_reversed(self, cls):
        self.assertListEqual(
            cls.of((0, 1, 2, 3, 4)).reversed().to_list(),
            [4, 3, 2, 1, 0]
        )

    @parametrized([Stream], [ParallelStream])
    def test_sorted(self, cls):
        self.assertListEqual(
            cls.of((2, 1, 4, 0, 3)).sorted().to_list(),
            [0, 1, 2, 3, 4]
        )

    @parametrized([Stream], [ParallelStream])
    def test_sum(self, cls):
        self.assertEqual(
            cls.of((0, 1, 2, 3, 4)).sum(),
            10
        )

    @parametrized([Stream], [ParallelStream])
    def test_summary_statistics(self, cls):
        self.assertEqual(
            cls.of((0, 1, 2, 3, 4)).summary_statistics(),
            namedtuple('SummaryStatistics', ['average', 'count', 'min', 'max', 'sum'])(2, 5, 0, 4, 10)
        )

    @parametrized([Stream], [ParallelStream])
    def test_to_dict(self, cls):
        self.assertDictEqual(
            cls.of((0, 1, 2, 3, 4)).to_dict(lambda x: x, lambda x: x + 1),
            {0: 1, 1: 2, 2: 3, 3: 4, 4: 5}
        )

    @parametrized([Stream], [ParallelStream])
    def test_to_list(self, cls):
        self.assertListEqual(
            cls.of((0, 1, 2, 3, 4)).to_list(),
            [0, 1, 2, 3, 4]
        )

    @parametrized([Stream], [ParallelStream])
    def test_to_set(self, cls):
        self.assertSetEqual(
            cls.of((0, 1, 2, 3, 4)).to_set(),
            {0, 1, 2, 3, 4}
        )


if __name__ == '__main__':
    unittest.main()
