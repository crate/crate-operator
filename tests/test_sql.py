# CrateDB Kubernetes Operator
#
# Licensed to Crate.IO GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.

from crate.operator.sql import (
    SQLResult,
    normalize_crash,
    normalize_crate_control,
    parse_crash_table,
)


class TestSQLResult:
    def test_ok_property_true(self):
        result = SQLResult(rowcount=1, rows=None, error_code=None, error_message=None)
        assert result.ok is True

    def test_ok_property_false(self):
        result = SQLResult(
            rowcount=None, rows=None, error_code=4000, error_message="Error"
        )
        assert result.ok is False


class TestNormalizeCrash:
    def test_alter_ok(self):
        result = normalize_crash("ALTER OK, 1 row affected (0.123 sec)")
        assert result.ok is True
        assert result.rowcount == 1
        assert result.error_message is None

    def test_create_ok(self):
        result = normalize_crash("CREATE OK, 1 row affected (0.123 sec)")
        assert result.ok is True
        assert result.rowcount == 1

    def test_grant_ok(self):
        result = normalize_crash("GRANT OK, 1 row affected (0.123 sec)")
        assert result.ok is True
        assert result.rowcount == 1

    def test_exception(self):
        result = normalize_crash(
            "SQLActionException[UserAlreadyExistsException: User 'test' already exists]"
        )
        assert result.ok is False
        assert result.error_message is not None
        assert "Exception" in result.error_message

    def test_select_with_results(self):
        output = """
+------+-------+
| col1 | col2  |
+------+-------+
| 1    | test  |
| 2    | hello |
+------+-------+
SELECT 2 rows in set (0.123 sec)
"""
        result = normalize_crash(output)
        assert result.ok is True
        assert result.rowcount == 2
        assert result.rows is not None
        assert len(result.rows) == 2
        assert result.rows[0] == (1, "test")
        assert result.rows[1] == (2, "hello")


class TestParseCrashTable:
    def test_parse_simple_table(self):
        output = """
+------+-------+
| col1 | col2  |
+------+-------+
| 1    | test  |
| 2    | hello |
+------+-------+
"""
        rows = parse_crash_table(output)
        assert len(rows) == 2
        assert rows[0] == (1, "test")
        assert rows[1] == (2, "hello")

    def test_parse_empty_table(self):
        output = """
+------+
| col1 |
+------+
+------+
"""
        rows = parse_crash_table(output)
        assert rows == []

    def test_parse_with_null_values(self):
        """NULL values (empty strings) should be converted to None."""
        output = """
+------+-------+
| col1 | col2  |
+------+-------+
| 1    |       |
+------+-------+
"""
        rows = parse_crash_table(output)
        assert rows[0] == (1, None)

    def test_parse_with_boolean(self):
        """Boolean values should be converted."""
        output = """
+------+-------+
| col1 | col2  |
+------+-------+
| true | false |
+------+-------+
"""
        rows = parse_crash_table(output)
        assert rows[0] == (True, False)

    def test_parse_with_float(self):
        """Float values should be converted."""
        output = """
+-------+
| col1  |
+-------+
| 3.14  |
| -2.5  |
+-------+
"""
        rows = parse_crash_table(output)
        assert rows[0] == (3.14,)
        assert rows[1] == (-2.5,)


class TestNormalizeCrateControl:
    def test_success_response(self):
        response = {"rowcount": 1, "rows": [[1, "test"]]}
        result = normalize_crate_control(response)
        assert result.ok is True
        assert result.rowcount == 1
        assert result.rows is not None
        assert result.rows == [[1, "test"]]
        assert result.error_message is None

    def test_error_response_with_error_key(self):
        response = {"error": {"code": 4000, "message": "User already exists"}}
        result = normalize_crate_control(response)
        assert result.ok is False
        assert result.error_code == 4000
        assert result.error_message == "User already exists"

    def test_error_response_with_detail_key(self):
        response = {"detail": '{"error": {"code": 4000, "message": "Error"}}'}
        result = normalize_crate_control(response)
        assert result.ok is False
        assert result.error_code == 4000
        assert result.error_message == "Error"

    def test_error_response_with_string_detail(self):
        response = {"detail": "Something went wrong"}
        result = normalize_crate_control(response)
        assert result.ok is False
        assert result.error_message == "Something went wrong"
