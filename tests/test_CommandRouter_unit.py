import unittest
from datetime import datetime
from my_classes.CommandRouter import CommandRouter
from my_classes.DataBaseService import DataBaseService

class TestCommandRouter (unittest.TestCase):
    
    def setUp(self):
        self.database=DataBaseService(database="test_mailbox")
        self.router=CommandRouter("x.x.x", datetime(2024, 1, 1, 12, 0, 0),self.database)

    def test_info_command(self):
        result=self.router.handle_command("info")
        self.assertIn("Server version: x.x.x",result)

    def test_uptime_command(self):
        result=self.router.handle_command("uptime")
        self.assertIn("Server uptime:", result)

    def test_help_command(self):
        result=self.router.handle_command("help")
        self.assertIsInstance(result,dict)
        self.assertIn("help", result)
        self.assertIn("Available command", result["help"])
