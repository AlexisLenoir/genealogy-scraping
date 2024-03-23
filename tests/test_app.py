import pytest
import sys

# sys.path.append('/Users/alexislenoir/python3.10_ws/extract_roglo/sam-app/src')
sys.path.append("sam-app/src")
import app
import os
from bs4 import BeautifulSoup


# ------------- Test get_id() -------------
@pytest.mark.parametrize(
    "test_input,expected",
    [("roglo?lang=fr;i=1640524", "1640524"), ("roglo?lang=fr;i=1234567", "1234567")],
)
def test_get_id(test_input, expected):
    assert app.get_id(test_input) == expected


# ------------- Load a parsed file example -------------
def get_parsed_file_example():
    with open("sam-app/tests/example_person_page.html", "r") as file:
        return BeautifulSoup(file.read(), "html.parser")


# ------------- Test get_names() -------------
def test_get_names():
    assert app.get_names(get_parsed_file_example()) == ("charles", "de gaulle")


# ------------- Test get_parent_id() -------------
def test_get_parent_id():
    assert app.get_parent_id(get_parsed_file_example()) == (3481126, 3481008)


# ------------- Test test_parents_existence() -------------
def test_test_parents_existence():
    assert app.test_parents_existence(get_parsed_file_example()) == True


print(os.getcwd())
