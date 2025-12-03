from datetime import datetime
from unittest.mock import patch
from app.dashboard import days_left_in_month_func

@patch('app.dashboard.datetime') 
def test_middle_of_month(mock_datetime):
    mock_datetime.now.return_value = datetime(2024, 1, 15)
    result = days_left_in_month_func()
    assert result == 16

@patch('app.dashboard.datetime')
def test_last_day_returns_next_month_days(mock_datetime):
    mock_datetime.now.return_value = datetime(2024, 1, 31)
    result = days_left_in_month_func()
    assert result == 29  # Feb 2024 has 29 days

@patch('app.dashboard.datetime')
def test_december_to_january(mock_datetime):
    mock_datetime.now.return_value = datetime(2024, 12, 31)
    result = days_left_in_month_func()
    assert result == 31  # January has 31 days