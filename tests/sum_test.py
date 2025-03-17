import unittest
from app.main import sum

class TestSumFunction(unittest.TestCase):
    def test_sum_positive_numbers(self):
        self.assertEqual(sum(1, 2), 3)
    
    def test_sum_negative_numbers(self):
        self.assertEqual(sum(-1, -2), -3)
    
    def test_sum_positive_and_negative(self):
        self.assertEqual(sum(1, -2), -1)
    
    def test_sum_zero(self):
        self.assertEqual(sum(0, 0), 0)

if __name__ == "__main__":
    unittest.main()