import pandas as pd

from app.services.data_service import execute_query_code


def test_execute_query_code_converts_dtypes_to_strings() -> None:
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    # LLMs often produce code that returns df.dtypes; those are numpy.dtype values.
    code = "result = df.dtypes"

    result = execute_query_code(code, df)

    assert isinstance(result, dict)
    assert result["a"] == "int64" or isinstance(result["a"], str)
    assert result["b"] == "object" or isinstance(result["b"], str)


def test_execute_query_code_returns_print_output_when_no_result() -> None:
    df = pd.DataFrame({"sentiment": ["pos", "neg", "pos"], "review": ["a", "b", "c"]})

    code = "print(df['sentiment'].value_counts())"
    result = execute_query_code(code, df)

    assert isinstance(result, str)
    assert "pos" in result
