import pytest

from app.services.query_classifier import classify_query


@pytest.mark.parametrize(
    "question,expected",
    [
        ("hi", "smalltalk"),
        ("Hello there!", "smalltalk"),
        ("thanks for your help", "smalltalk"),
        ("what are you?", "smalltalk"),
        ("tell me about the data", "descriptive"),
        ("Summarize the dataset", "descriptive"),
        ("what columns exist", "descriptive"),
        ("show schema", "descriptive"),
        ("which product sold most", "analytical"),
        ("Compare average sentiment by month", "analytical"),
    ],
)
def test_classify_query_basic(question: str, expected: str) -> None:
    schema = {"columns": ["review", "sentiment", "product", "month", "sales"]}
    assert classify_query(question, schema) == expected


def test_empty_question_fallbacks_to_analytical() -> None:
    assert classify_query("", {"columns": ["a"]}) == "analytical"


def test_whitespace_question_fallbacks_to_analytical() -> None:
    assert classify_query("   ", {"columns": ["a"]}) == "analytical"


def test_descriptive_missing_duplicates() -> None:
    schema = {"columns": ["a", "b"]}
    assert classify_query("summarize missing values and duplicates", schema) == "descriptive"


def test_analytical_when_column_name_mentioned() -> None:
    schema = {"columns": ["total_sales", "product"]}
    assert classify_query("What is the total_sales for each product?", schema) == "analytical"


def test_mixed_smalltalk_and_data_prefers_smalltalk() -> None:
    schema = {"columns": ["a", "b"]}
    assert classify_query("hi, what columns exist?", schema) == "smalltalk"
