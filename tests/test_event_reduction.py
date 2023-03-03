from unittest import TestCase

from versioned_collection.utils.events import reduce_event_sequence


class TestEventReduction(TestCase):

    def test_empty_sequence(self):
        with self.assertRaises(ValueError):
            reduce_event_sequence([])

    def test_none_sequence(self):
        with self.assertRaises(ValueError):
            reduce_event_sequence(None)  # noqa

    def test_single_element_sequence(self):
        event = 'i'
        res = reduce_event_sequence([event])
        self.assertEqual(event, res)

    def test_single_element_invalid_seq(self):
        with self.assertRaises(ValueError):
            reduce_event_sequence(['r'])

    def test_invalid_element_inside_seq(self):
        with self.assertRaises(ValueError):
            reduce_event_sequence(['i', 'u', 'r'])

    def test_invalid_element_seq_1(self):
        with self.assertRaises(ValueError):
            reduce_event_sequence(['i', 'i'])

    def test_invalid_element_seq_2(self):
        with self.assertRaises(ValueError):
            reduce_event_sequence(['d', 'd', 'i'])

    def test_invalid_element_seq_3(self):
        with self.assertRaises(ValueError):
            reduce_event_sequence(['d', 'u'])

    def test_invalid_element_seq_4(self):
        with self.assertRaises(ValueError):
            reduce_event_sequence(['u', 'i'])

    def test_invalid_element_seq_5(self):
        with self.assertRaises(ValueError):
            reduce_event_sequence([1, 2, 3])  # noqa

    def test_reduction_1(self):
        self.assertIsNone(reduce_event_sequence(['i', 'u', 'u', 'd']))

    def test_reduction_2(self):
        self.assertIsNone(reduce_event_sequence(['i', 'd', 'u', 'd']))

    def test_reduction_3(self):
        self.assertEqual('i', reduce_event_sequence(['i', 'd', 'i', 'u']))

    def test_reduction_4(self):
        self.assertEqual('i', reduce_event_sequence(['i', 'u', 'u', 'u']))

    def test_reduction_5(self):
        self.assertEqual('u', reduce_event_sequence(['d', 'i', 'u']))

    def test_reduction_6(self):
        self.assertEqual('u', reduce_event_sequence(['u', 'u', 'u']))

    def test_reduction_7(self):
        self.assertEqual('d', reduce_event_sequence(['u', 'u', 'd']))

    def test_reduction_8(self):
        self.assertEqual(
            'i',
            reduce_event_sequence(['i', 'u', 'd', 'i', 'u', 'u', 'd', 'i', 'u'])
        )

    def test_reduction_9(self):
        self.assertEqual('u', reduce_event_sequence(['d', 'i', 'u']))

    def test_reduction_10(self):
        self.assertEqual('u', reduce_event_sequence(['d', 'i']))
        self.assertEqual('u', reduce_event_sequence(['u', 'u']))
        self.assertEqual('d', reduce_event_sequence(['u', 'd']))
        self.assertEqual(None, reduce_event_sequence(['i', 'd']))
        self.assertEqual('i', reduce_event_sequence(['i', 'u']))
