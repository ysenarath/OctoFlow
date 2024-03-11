import unittest

import pandas as pd

from octoflow.data.compute import Expression
from octoflow.data.dataset import Dataset


class DatasetTestCase(unittest.TestCase):
    def test_create_dataset_from_list_of_dicts(self):
        data = [
            {"name": "John", "age": 25},
            {"name": "Jane", "age": 30},
            {"name": "Bob", "age": 35},
        ]
        dataset = Dataset(data)
        self.assertEqual(len(dataset), 3)

    def test_create_dataset_from_dict_of_lists(self):
        data = {
            "name": ["John", "Jane", "Bob"],
            "age": [25, 30, 35],
        }
        dataset = Dataset(data)
        self.assertEqual(len(dataset), 3)

    def test_create_dataset_from_dataframe(self):
        data = pd.DataFrame({
            "name": ["John", "Jane", "Bob"],
            "age": [25, 30, 35],
        })
        dataset = Dataset(data)
        self.assertEqual(len(dataset), 3)

    def test_filter_dataset(self):
        persons = [
            {"name": "John", "age": 25},
            {"name": "Jane", "age": 30},
            {"name": "Bob", "age": 35},
        ]
        persons_dset = Dataset(persons)
        filtered_persons_dset = persons_dset.filter(
            Expression.field("age") > 30
        )
        self.assertEqual(len(filtered_persons_dset), 1)
        person = filtered_persons_dset[0]
        self.assertEqual(person["name"], "Bob")

    def test_map_dataset(self):
        persons = [
            {"name": "John", "age": 25},
            {"name": "Jane", "age": 30},
            {"name": "Bob", "age": 35},
        ]
        person_dset = Dataset(persons)
        mapped_person_dset = person_dset.map(
            lambda row: {"name": row["name"].upper(), "age": row["age"] + 1}
        )
        self.assertEqual(len(mapped_person_dset), 3)
        person = mapped_person_dset[0]
        self.assertEqual(person["name"], "JOHN")
        self.assertEqual(person["age"], 26)


if __name__ == "__main__":
    unittest.main()
