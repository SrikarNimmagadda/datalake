import pytest

def pytest_addoption(parser):
    parser.addoption("--stackname", action="store", default="default stack name")


def pytest_generate_tests(metafunc):
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".
    option_value = metafunc.config.option.stackname
    if 'stackname' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("stackname", [option_value])