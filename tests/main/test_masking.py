import hashlib
import math
import random
import unittest
from datetime import date, datetime
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet
import pytest

from omniload.codec.masking import MaskingEngine, create_masking_mapper
from tests.util import invoke_ingest_command


class TestMaskingEngine(unittest.TestCase):
    def setUp(self):
        self.engine = MaskingEngine()
        # Set seed for reproducible tests
        random.seed(42)

    def test_parse_mask_config(self):
        # Test basic config
        col, algo, param = self.engine.parse_mask_config("email:hash")
        self.assertEqual(col, "email")
        self.assertEqual(algo, "hash")
        self.assertIsNone(param)

        # Test config with parameter
        col, algo, param = self.engine.parse_mask_config("age:round:10")
        self.assertEqual(col, "age")
        self.assertEqual(algo, "round")
        self.assertEqual(param, "10")

        # Test invalid config
        with self.assertRaises(ValueError):
            self.engine.parse_mask_config("invalid")

    def test_hash_sha256(self):
        result = self.engine._hash_sha256("test@example.com")
        self.assertEqual(
            result, "973dfe463ec85785f5f95af5ba3906eedb2d931c24e69824a89ea65dba4e813b"
        )

        # Test consistency
        result2 = self.engine._hash_sha256("test@example.com")
        self.assertEqual(result, result2)

        # Test None handling
        self.assertIsNone(self.engine._hash_sha256(None))

    def test_hash_md5(self):
        result = self.engine._hash_md5("test@example.com")
        self.assertEqual(result, "55502f40dc8b7c769880b10874abc9d0")

        # Test None handling
        self.assertIsNone(self.engine._hash_md5(None))

    def test_hash_hmac(self):
        result = self.engine._hash_hmac("test@example.com", "secret-key")
        # HMAC should produce consistent results with same key
        result2 = self.engine._hash_hmac("test@example.com", "secret-key")
        self.assertEqual(result, result2)

        # Different keys should produce different results
        result3 = self.engine._hash_hmac("test@example.com", "different-key")
        self.assertNotEqual(result, result3)

    def test_mask_email(self):
        result = self.engine._mask_email("john.doe@example.com")
        self.assertTrue(result.endswith("@example.com"))
        self.assertTrue("*" in result)

        # Test short email
        result = self.engine._mask_email("ab@test.com")
        self.assertEqual(result, "**@test.com")

        # Test invalid email (no @)
        result = self.engine._mask_email("notanemail")
        self.assertTrue("*" in result)

        # Test None handling
        self.assertIsNone(self.engine._mask_email(None))

    def test_mask_phone(self):
        result = self.engine._mask_phone("555-123-4567")
        self.assertEqual(result, "555-***-****")

        result = self.engine._mask_phone("+1-555-123-4567")
        self.assertTrue("***" in result)

        result = self.engine._mask_phone("12345")
        self.assertEqual(result, "*****")

        self.assertIsNone(self.engine._mask_phone(None))

    def test_mask_credit_card(self):
        result = self.engine._mask_credit_card("4111-1111-1111-1111")
        self.assertEqual(result, "************1111")

        result = self.engine._mask_credit_card("4111111111111111")
        self.assertEqual(result, "************1111")

        result = self.engine._mask_credit_card("1234")
        self.assertEqual(result, "****")

        self.assertIsNone(self.engine._mask_credit_card(None))

    def test_mask_ssn(self):
        result = self.engine._mask_ssn("123-45-6789")
        self.assertEqual(result, "***-**-6789")

        result = self.engine._mask_ssn("123456789")
        self.assertEqual(result, "***-**-6789")

        result = self.engine._mask_ssn("12345")
        self.assertEqual(result, "*****")

        self.assertIsNone(self.engine._mask_ssn(None))

    def test_partial_mask(self):
        result = self.engine._partial_mask("Jonathan", 2)
        self.assertEqual(result, "Jo****an")

        result = self.engine._partial_mask("test", 1)
        self.assertEqual(result, "t**t")

        result = self.engine._partial_mask("ab", 2)
        self.assertEqual(result, "**")

        self.assertIsNone(self.engine._partial_mask(None, 2))

    def test_first_letter_mask(self):
        result = self.engine._first_letter_mask("Alice")
        self.assertEqual(result, "A****")

        result = self.engine._first_letter_mask("B")
        self.assertEqual(result, "B")

        # Test None handling
        self.assertIsNone(self.engine._first_letter_mask(None))

    def test_tokenize_uuid(self):
        result1 = self.engine._tokenize_uuid("customer1")
        result2 = self.engine._tokenize_uuid("customer1")

        # Same value should get same UUID
        self.assertEqual(result1, result2)

        # Different values should get different UUIDs
        result3 = self.engine._tokenize_uuid("customer2")
        self.assertNotEqual(result1, result3)

        # Test None handling
        self.assertIsNone(self.engine._tokenize_uuid(None))

    def test_tokenize_sequential(self):
        engine = MaskingEngine()  # Fresh engine for clean counter
        result1 = engine._tokenize_sequential("customer1")
        self.assertEqual(result1, 1)

        result2 = engine._tokenize_sequential("customer2")
        self.assertEqual(result2, 2)

        # Same value should get same sequential ID
        result3 = engine._tokenize_sequential("customer1")
        self.assertEqual(result3, 1)

        # Test None handling
        self.assertIsNone(engine._tokenize_sequential(None))

    def test_round_number(self):
        result = self.engine._round_number(52300, 5000)
        self.assertEqual(result, 50000)

        result = self.engine._round_number(34, 10)
        self.assertEqual(result, 30)

        result = self.engine._round_number(37.5, 10)
        self.assertEqual(result, 40)

        # Test non-numeric value
        result = self.engine._round_number("not_a_number", 10)
        self.assertEqual(result, "not_a_number")

        # Test None handling
        self.assertIsNone(self.engine._round_number(None, 10))

    def test_range_mask(self):
        result = self.engine._range_mask(45000, 10000)
        self.assertEqual(result, "40000-50000")

        result = self.engine._range_mask(234, 100)
        self.assertEqual(result, "200-300")

        # Test non-numeric value
        result = self.engine._range_mask("not_a_number", 100)
        self.assertEqual(result, "not_a_number")

        # Test None handling
        self.assertIsNone(self.engine._range_mask(None, 100))

    def test_add_noise(self):
        # Reset seed for consistent test
        random.seed(42)
        result = self.engine._add_noise(100, 0.1)
        # Result should be within 10% of original
        self.assertTrue(90 <= result <= 110)

        # Test integer preservation
        result = self.engine._add_noise(100, 0.1)
        self.assertIsInstance(result, int)

        # Test float preservation
        result = self.engine._add_noise(100.5, 0.1)
        self.assertIsInstance(result, float)

        # Test non-numeric value
        result = self.engine._add_noise("not_a_number", 0.1)
        self.assertEqual(result, "not_a_number")

        # Test None handling
        self.assertIsNone(self.engine._add_noise(None, 0.1))

    def test_year_only(self):
        test_date = date(2024, 3, 15)
        result = self.engine._year_only(test_date)
        self.assertEqual(result, 2024)

        test_datetime = datetime(2024, 3, 15, 10, 30)
        result = self.engine._year_only(test_datetime)
        self.assertEqual(result, 2024)

        # Test string date
        result = self.engine._year_only("2024-03-15")
        self.assertEqual(result, 2024)

        # Test invalid date string
        result = self.engine._year_only("not_a_date")
        self.assertEqual(result, "not_a_date")

        # Test None handling
        self.assertIsNone(self.engine._year_only(None))

    def test_month_year(self):
        test_date = date(2024, 3, 15)
        result = self.engine._month_year(test_date)
        self.assertEqual(result, "2024-03")

        test_datetime = datetime(2024, 3, 15, 10, 30)
        result = self.engine._month_year(test_datetime)
        self.assertEqual(result, "2024-03")

        # Test string date
        result = self.engine._month_year("2024-03-15")
        self.assertEqual(result, "2024-03")

        # Test invalid date string
        result = self.engine._month_year("not_a_date")
        self.assertEqual(result, "not_a_date")

        # Test None handling
        self.assertIsNone(self.engine._month_year(None))

    def test_get_masking_function(self):
        # Test hash algorithms
        func = self.engine.get_masking_function("hash")
        self.assertIsNotNone(func)

        func = self.engine.get_masking_function("sha256")
        self.assertIsNotNone(func)

        func = self.engine.get_masking_function("md5")
        self.assertIsNotNone(func)

        # Test with parameters
        func = self.engine.get_masking_function("partial", "3")
        result = func("testing")
        self.assertEqual(result, "tes*ing")

        func = self.engine.get_masking_function("round", "100")
        result = func(456)
        self.assertEqual(result, 500)

        # Test fixed value
        func = self.engine.get_masking_function("fixed", "MASKED")
        result = func("anything")
        self.assertEqual(result, "MASKED")

        # Test unknown algorithm
        with self.assertRaises(ValueError):
            self.engine.get_masking_function("unknown_algo")


class TestCreateMaskingMapper(unittest.TestCase):
    def test_create_masking_mapper_basic(self):
        mask_configs = ["email:hash", "phone:partial:3", "ssn:redact"]

        mapper = create_masking_mapper(mask_configs)

        row = {
            "email": "test@example.com",
            "phone": "555-123-4567",
            "ssn": "123-45-6789",
            "name": "John Doe",
        }

        result = mapper(row)

        # Email should be hashed
        self.assertEqual(
            result["email"],
            "973dfe463ec85785f5f95af5ba3906eedb2d931c24e69824a89ea65dba4e813b",
        )

        # Phone should be partially masked
        self.assertTrue("*" in result["phone"])

        # SSN should be redacted
        self.assertEqual(result["ssn"], "REDACTED")

        # Name should be unchanged
        self.assertEqual(result["name"], "John Doe")

    def test_create_masking_mapper_empty_config(self):
        mapper = create_masking_mapper([])

        row = {"email": "test@example.com"}
        result = mapper(row)

        # Should return unchanged
        self.assertEqual(result, row)

    def test_create_masking_mapper_non_dict_row(self):
        mask_configs = ["email:hash"]
        mapper = create_masking_mapper(mask_configs)

        # Non-dict should be returned unchanged
        result = mapper("not a dict")
        self.assertEqual(result, "not a dict")

        result = mapper(None)
        self.assertIsNone(result)

        result = mapper(123)
        self.assertEqual(result, 123)

    def test_create_masking_mapper_missing_column(self):
        mask_configs = ["email:hash", "phone:redact"]
        mapper = create_masking_mapper(mask_configs)

        row = {"name": "John Doe"}  # No email or phone
        result = mapper(row)

        # Should not add new columns, just return unchanged
        self.assertEqual(result, {"name": "John Doe"})

    def test_create_masking_mapper_with_numeric_masks(self):
        mask_configs = ["age:round:10", "salary:range:10000", "score:noise:0.1"]

        # Set seed for reproducible noise test
        random.seed(42)

        mapper = create_masking_mapper(mask_configs)

        row = {"age": 34, "salary": 45000, "score": 100}

        result = mapper(row)

        self.assertEqual(result["age"], 30)
        self.assertEqual(result["salary"], "40000-50000")
        # Score should be within noise range
        self.assertTrue(90 <= result["score"] <= 110)

    @patch("builtins.print")
    def test_create_masking_mapper_error_handling(self, mock_print):
        mask_configs = ["value:round:10"]  # round expects numeric
        mapper = create_masking_mapper(mask_configs)

        row = {"value": "not_a_number"}
        result = mapper(row)

        # Should handle error gracefully and return unchanged
        self.assertEqual(result["value"], "not_a_number")

        # But for valid numeric it should work
        row2 = {"value": 123}
        result2 = mapper(row2)
        self.assertEqual(result2["value"], 120)

    def test_create_masking_mapper_arrow_items(self):
        import pyarrow as pa

        mapper = create_masking_mapper(["email:hash"])
        data = {"email": ["test@example.com"], "name": ["John Doe"]}

        for item in (pa.table(data), pa.record_batch(data)):
            result = mapper(item)

            self.assertIs(type(result), type(item))
            self.assertEqual(
                result.column("email").to_pylist(),
                ["973dfe463ec85785f5f95af5ba3906eedb2d931c24e69824a89ea65dba4e813b"],
            )
            self.assertEqual(result.column("name").to_pylist(), ["John Doe"])

    def test_create_masking_mapper_arrow_items_keep_unmasked_columns(self):
        import pyarrow as pa

        mapper = create_masking_mapper(["email:hash"])
        schema = pa.schema(
            [
                pa.field("email", pa.string()),
                pa.field("id", pa.int64(), nullable=False, metadata={"k": "v"}),
                pa.field("big", pa.int64()),
            ],
            metadata={"source": "test"},
        )
        data = {
            "email": ["a@example.com", None],
            "id": [1, 2],
            "big": [2**53 + 1, None],
        }

        for item in (
            pa.table(data, schema=schema),
            pa.record_batch(data, schema=schema),
        ):
            result = mapper(item)

            self.assertEqual(result.schema.metadata, schema.metadata)
            self.assertEqual(result.schema.field("id"), schema.field("id"))
            self.assertEqual(result.schema.field("big"), schema.field("big"))
            self.assertEqual(result.column("big").to_pylist(), [2**53 + 1, None])

    def test_create_masking_mapper_arrow_item_without_masked_column(self):
        import pyarrow as pa

        mapper = create_masking_mapper(["email:hash"])
        for item in (pa.table({"id": [1]}), pa.record_batch({"id": [1]})):
            self.assertIs(mapper(item), item)


class TestMaskingArrowParity(unittest.TestCase):
    """Arrow items mask the same values, in the same order, as before."""

    def test_a_null_stays_null(self):
        import pandas as pd
        import pyarrow as pa

        frame = pd.DataFrame(
            {
                "e": pd.array(["a@example.com", None], dtype="object"),
                "v": pd.array([1, None], dtype="Int64"),
            }
        )
        items = [
            pa.table({"e": ["a@example.com", None], "v": [1, None]}),
            pa.record_batch({"e": ["a@example.com", None], "v": [1, None]}),
            pa.Table.from_pandas(frame),
            pa.RecordBatch.from_pandas(frame),
        ]
        for item in items:
            result = create_masking_mapper(["e:sha256", "v:sha256"])(item)

            self.assertEqual(
                result.column("e").to_pylist(),
                [hashlib.sha256(b"a@example.com").hexdigest(), None],
            )
            self.assertEqual(
                result.column("v").to_pylist(),
                [hashlib.sha256(b"1").hexdigest(), None],
            )
            self.assertNotIn(b"pandas", result.schema.metadata or {})
            result.to_pandas()

    def test_an_integer_hashes_the_same_with_and_without_a_null_beside_it(self):
        import pyarrow as pa

        mapper = create_masking_mapper(["v:sha256"])
        expected = [
            hashlib.sha256(str(2**53 + 1).encode()).hexdigest(),
            hashlib.sha256(b"7").hexdigest(),
        ]
        for kind in (pa.table, pa.record_batch):
            without_null = mapper(kind({"v": pa.array([2**53 + 1, 7], pa.int64())}))
            with_null = mapper(kind({"v": pa.array([2**53 + 1, 7, None], pa.int64())}))

            self.assertEqual(without_null.column("v").to_pylist(), expected)
            self.assertEqual(with_null.column("v").to_pylist(), [*expected, None])
            self.assertEqual(mapper({"v": 2**53 + 1})["v"], expected[0])

    def test_sequential_tokens_follow_the_mask_order(self):
        import pyarrow as pa

        configs = ["b:sequential", "a:sequential"]
        row = create_masking_mapper(configs)({"a": "alice", "b": "bob"})
        for item in (
            pa.table({"a": ["alice"], "b": ["bob"]}),
            pa.record_batch({"a": ["alice"], "b": ["bob"]}),
        ):
            result = create_masking_mapper(configs)(item)

            self.assertEqual(
                (result.column("a")[0].as_py(), result.column("b")[0].as_py()),
                (row["a"], row["b"]),
            )

    def test_a_column_pandas_metadata_makes_the_index_is_masked(self):
        # pandas would read such a column back as the index, but it is an ordinary
        # column of the Arrow item and loads like one, so it is masked like one.
        import pandas as pd
        import pyarrow as pa

        frame = pd.DataFrame({"v": ["x"]}, index=pd.Index(["secret"], name="email"))
        for item in (
            pa.Table.from_pandas(frame),
            pa.RecordBatch.from_pandas(frame),
        ):
            result = create_masking_mapper(["email:redact", "v:redact"])(item)

            self.assertEqual(result.column("email").to_pylist(), ["REDACTED"])
            self.assertEqual(result.column("v").to_pylist(), ["REDACTED"])
            self.assertNotIn(b"pandas", result.schema.metadata or {})

    def test_a_column_the_mask_returns_unchanged_is_kept_as_it_is(self):
        import pyarrow as pa

        stamp = pa.scalar(1_700_000_000_123_456_789, pa.timestamp("ns", tz="UTC"))
        # Each mask here does not apply to the values and returns them as they are.
        cases = [
            ("year_only", pa.array([2**63 + 1, None], pa.uint64())),
            ("round", pa.array([stamp, None], stamp.type)),
            ("round", pa.array([stamp, stamp, None], stamp.type).dictionary_encode()),
            ("round", pa.array([[stamp.as_py()], None], pa.list_(stamp.type))),
            ("round", pa.array([12_345, None], pa.time64("ns"))),
            (
                "round",
                pa.array([[], [("x", 1)], None], pa.map_(pa.string(), pa.int64())),
            ),
        ]
        for algorithm, array in cases:
            field = pa.field("v", array.type, metadata={"k": "v"})
            for kind in (pa.Table, pa.RecordBatch):
                item = kind.from_arrays([array], schema=pa.schema([field]))
                result = create_masking_mapper([f"v:{algorithm}"])(item)

                self.assertEqual(result.schema.field("v"), field)
                self.assertTrue(result.column("v").equals(item.column("v")))

    def test_an_all_null_masked_column_takes_the_null_type(self):
        # Kept, it would carry its source type into a load whose other batches
        # carry the mask's, which dlt refuses to put together. The unmasked
        # column keeps the batch's rows when dlt drops the `null` one.
        import pyarrow as pa

        for array in (
            pa.array([None, None], pa.int64()),
            pa.array([None, None], pa.string()),
            pa.array([None], pa.int64()).dictionary_encode(),
        ):
            for kind in (pa.Table, pa.RecordBatch):
                ids = pa.array(range(len(array)))
                item = kind.from_arrays([array, ids], ["v", "id"])
                result = create_masking_mapper(["v:sha256"])(item)

                self.assertEqual(result.schema.field("v").type, pa.null())
                self.assertEqual(result.column("v").null_count, len(array))

    def test_an_all_null_masked_column_takes_the_type_the_mask_gave_it_before(self):
        # Whether the mask changes the type (sha256) or hands the values back
        # (round), a later all-NULL column matches what earlier items carried.
        import pyarrow as pa

        day = date(2024, 1, 2)
        cases = [
            ("sha256", pa.array([1, 2], pa.int64())),
            ("round", pa.array([day, day], pa.date64())),
            ("round", pa.array([day, day]).dictionary_encode()),
            ("round", pa.array([[1], [2]], pa.list_(pa.int64()))),
        ]
        for algorithm, values in cases:
            for kind in (pa.Table, pa.RecordBatch):
                mapper = create_masking_mapper([f"v:{algorithm}"])
                first = mapper(kind.from_arrays([values], ["v"]))
                nulls = pa.nulls(3, values.type)
                second = mapper(kind.from_arrays([nulls], ["v"]))

                self.assertEqual(
                    second.schema.field("v").type, first.schema.field("v").type
                )
                self.assertEqual(second.column("v").null_count, 3)

    def test_mappers_sharing_an_engine_share_tokens_but_not_remembered_types(self):
        # One mapper per resource: what a mask gave one table's column does not
        # type another table's, while a value keeps one token across tables.
        import pyarrow as pa

        from omniload.codec.masking import MaskingEngine

        engine = MaskingEngine()
        first = create_masking_mapper(["v:round", "t:sequential"], engine)
        second = create_masking_mapper(["v:round", "t:sequential"], engine)

        numbers = first(pa.table({"v": ["10", "20"], "t": ["x", "y"]}))
        nulls = second(pa.table({"v": pa.nulls(2, pa.string()), "t": ["y", "z"]}))

        self.assertEqual(numbers.schema.field("v").type, pa.int64())
        self.assertEqual(nulls.schema.field("v").type, pa.null())
        self.assertEqual(numbers.column("t").to_pylist(), [1, 2])
        self.assertEqual(nulls.column("t").to_pylist(), [2, 3])

    def test_a_remembered_type_is_matched_by_equality_not_by_its_text(self):
        # These two struct types print alike but are not equal: the second has one
        # field whose name is the first one's whole field list.
        import pyarrow as pa

        two_fields = pa.struct([("a", pa.int64()), ("b", pa.int64())])
        one_field = pa.struct([("a: int64, b", pa.int64())])
        self.assertEqual(str(two_fields), str(one_field))
        self.assertFalse(two_fields.equals(one_field))

        mapper = create_masking_mapper(["v:round"])
        mapper(pa.table({"v": pa.array([{"a": 1, "b": 2}], two_fields)}))
        nulls = mapper(pa.table({"v": pa.nulls(2, one_field), "id": [1, 2]}))

        self.assertEqual(nulls.schema.field("v").type, pa.null())

    def test_a_masked_number_column_takes_one_type_whatever_its_values(self):
        # dlt refuses batches of one resource whose types differ, so the type a
        # mask gives a column cannot depend on which values a batch holds:
        # `round:10` hands zeros back as the same objects and other values as new
        # ones, and a NaN-only batch rounds to NaN.
        import pyarrow as pa

        widths = {
            pa.int8(): 100,
            pa.int16(): 30_000,
            pa.int32(): 2_000_000,
            pa.int64(): 2**40,
            pa.uint8(): 200,
            pa.uint16(): 60_000,
            pa.uint32(): 4_000_000_000,
            pa.uint64(): 2**63 + 5,
        }
        cases = []
        for column_type, large in widths.items():
            batches = [[0, 0, 0], [10, 20, None], [large, 5]]
            for algorithm in ("round:10", "noise:0", "noise", "year_only"):
                cases.append((column_type, algorithm, batches))
        for column_type in (pa.float16(), pa.float32(), pa.float64()):
            batches = [[0.0, 0.0], [math.nan, math.nan], [1.5, None], [12_345.5, 9.0]]
            for algorithm in ("round:10", "noise:0", "noise", "range"):
                cases.append((column_type, algorithm, batches))
        for column_type, algorithm, batches in cases:
            # pyarrow builds a half-float array from Python floats only by a cast.
            build = pa.float64() if column_type == pa.float16() else column_type
            arrays = [pa.array(values, build).cast(column_type) for values in batches]
            for order in (arrays, arrays[::-1]):
                for kind in (pa.Table, pa.RecordBatch):
                    mapper = create_masking_mapper([f"v:{algorithm}"])
                    types = {
                        mapper(kind.from_arrays([array], ["v"])).schema.field("v").type
                        for array in order
                    }
                    types.discard(pa.null())
                    with self.subTest(column_type=column_type, algorithm=algorithm):
                        self.assertEqual(len(types), 1, types)

    def test_a_nan_only_batch_takes_the_type_the_mask_gave_it_before(self):
        # `round` and `range` hand a NaN back as it is. Typed `null`, a batch of
        # nothing else would lose its rows when it is all a batch holds.
        import pyarrow as pa

        for algorithm, masked_type in (
            ("round:10", pa.int64()),
            ("range", pa.string()),
        ):
            for kind in (pa.Table, pa.RecordBatch):
                mapper = create_masking_mapper([f"v:{algorithm}"])
                first = mapper(kind.from_arrays([pa.array([1.5, 22.5])], ["v"]))
                nans = mapper(kind.from_arrays([pa.array([math.nan] * 2)], ["v"]))

                self.assertEqual(first.schema.field("v").type, masked_type)
                self.assertEqual(nans.schema.field("v").type, masked_type)
                self.assertEqual(nans.column("v").null_count, 2)

    def test_a_batch_of_untyped_masked_columns_alone_keeps_its_source_kind(self):
        # dlt drops a `null` column, and a batch with no other column loses its
        # rows. Typed as its source kind loads, the batch keeps them; a later
        # batch the mask types otherwise fails the run instead.
        import pyarrow as pa

        cases = [
            (pa.array([None, None], pa.int32()), pa.int64()),
            (pa.array([None, None], pa.uint64()), pa.uint64()),
            (pa.array([math.nan, None], pa.float32()), pa.float64()),
            (pa.array([None, None], pa.large_string()), pa.string()),
            (pa.array([None, None], pa.string()).dictionary_encode(), pa.string()),
            (pa.array([None, None], pa.date32()), pa.date32()),
            (pa.array([None, None], pa.date32()).dictionary_encode(), pa.date32()),
        ]
        for array, source_kind in cases:
            for kind in (pa.Table, pa.RecordBatch):
                mapper = create_masking_mapper(["v:round", "w:round"])
                nulls = pa.nulls(2, pa.int64())
                item = kind.from_arrays([array, nulls], ["v", "w"])
                result = mapper(item)

                self.assertEqual(result.schema.field("v").type, source_kind)
                self.assertEqual(result.schema.field("w").type, pa.int64())
                self.assertEqual(result.num_rows, 2)
                self.assertEqual(result.column("v").null_count, 2)
                # Later all-NULL batches take the same type.
                again = mapper(kind.from_arrays([pa.nulls(1, array.type)], ["v"]))
                self.assertEqual(again.schema.field("v").type, source_kind)

    def test_an_empty_batch_of_untyped_masked_columns_stays_null(self):
        # No rows to keep: the batch loads beside any type the column takes later.
        import pyarrow as pa

        for kind in (pa.Table, pa.RecordBatch):
            item = kind.from_arrays([pa.array([], pa.int64())], ["v"])
            result = create_masking_mapper(["v:sha256"])(item)

            self.assertEqual(result.schema.field("v").type, pa.null())

    def test_a_timestamp_the_mask_hands_back_as_nat_is_kept_as_it_is(self):
        # The lowest nanosecond timestamp reads back as pandas' `NaT`, which
        # loads as NULL when a column is rebuilt; this one is not rebuilt.
        import pyarrow as pa

        array = pa.array([-(2**63), -(2**63)], pa.timestamp("ns"))
        for kind in (pa.Table, pa.RecordBatch):
            item = kind.from_arrays([array], ["v"])
            result = create_masking_mapper(["v:round"])(item)

            self.assertEqual(result.schema.field("v").type, array.type)
            self.assertTrue(result.column("v").equals(item.column("v")))

    def test_a_text_column_the_mask_returns_unchanged_takes_the_masked_type(self):
        # `first_letter` returns a one-letter value as it is and masks the rest;
        # the batch it leaves alone loads as `string` like the others.
        import pyarrow as pa

        for column_type in (pa.large_string(), pa.string_view()):
            mapper = create_masking_mapper(["v:first_letter"])
            short = mapper(pa.table({"v": pa.array(["a", "b"], column_type)}))
            long = mapper(pa.table({"v": pa.array(["abc", "b"], column_type)}))

            self.assertEqual(short.schema.field("v").type, pa.string())
            self.assertEqual(long.schema.field("v").type, pa.string())
            self.assertEqual(short.column("v").to_pylist(), ["a", "b"])

    def test_a_python_defined_extension_type_can_be_masked(self):
        import pyarrow as pa

        class IntExtension(pa.ExtensionType):
            def __init__(self):
                super().__init__(pa.int64(), "test.int_extension")

            def __arrow_ext_serialize__(self):
                return b""

            @classmethod
            def __arrow_ext_deserialize__(cls, storage_type, serialized):
                return cls()

        storage = pa.array([1, None], pa.int64())
        array = pa.ExtensionArray.from_storage(IntExtension(), storage)
        for algorithm in ("sha256", "round"):
            mapper = create_masking_mapper([f"v:{algorithm}"])
            result = mapper(pa.table({"v": array}))
            self.assertEqual(result.num_rows, 2)

    def test_masked_values_inference_cannot_hold_keep_the_column_type(self):
        import pyarrow as pa

        stamp = pa.scalar(1_700_000_000_123_456_789, pa.timestamp("ns", tz="UTC"))
        big = 2**63 + 2**61
        for kind in (pa.Table, pa.RecordBatch):
            # A shifted timestamp keeps its nanoseconds and time zone, decoded
            # from a dictionary too.
            for array in (
                pa.array([stamp, None], stamp.type),
                pa.array([stamp, stamp, None], stamp.type).dictionary_encode(),
            ):
                item = kind.from_arrays([array], ["v"])
                result = create_masking_mapper(["v:date_shift:0"])(item)

                self.assertEqual(result.schema.field("v").type, stamp.type)
                self.assertEqual(result.column("v")[0].value, stamp.value)
                self.assertEqual(result.column("v").to_pylist(), array.to_pylist())

            # A changed unsigned integer above the int64 range stays unsigned.
            item = kind.from_arrays([pa.array([big, None], pa.uint64())], ["v"])
            result = create_masking_mapper(["v:noise:0.01"])(item)

            self.assertEqual(result.schema.field("v").type, pa.uint64())
            value = result.column("v")[0].as_py()
            self.assertTrue(2**63 < value < 2**64 and value != big)

    def test_dates_beside_values_of_another_kind_are_refused(self):
        import pyarrow as pa

        # `date_shift` shifts what parses as a date and returns the rest unchanged,
        # which Arrow would read as microseconds since the epoch.
        for kind in (pa.Table, pa.RecordBatch):
            item = kind.from_arrays([pa.array([2024, 12345678, None])], ["v"])
            with self.assertRaisesRegex(ValueError, "dates or times"):
                create_masking_mapper(["v:date_shift"])(item)

    def test_a_datetime_beside_a_date_is_refused(self):
        import pyarrow as pa

        both = pa.UnionArray.from_dense(
            pa.array([0, 1], pa.int8()),
            pa.array([0, 0], pa.int32()),
            [pa.array([date(2024, 1, 2)]), pa.array([datetime(2024, 1, 2, 3, 4)])],
        )
        for kind in (pa.Table, pa.RecordBatch):
            item = kind.from_arrays([both], ["v"])
            with self.assertRaisesRegex(ValueError, "dates or times"):
                create_masking_mapper(["v:date_shift:0"])(item)

    def test_a_nan_beside_dates_reads_as_null(self):
        import pyarrow as pa

        # `date_shift` returns a NaN as it is, which loads as NULL.
        for kind in (pa.Table, pa.RecordBatch):
            item = kind.from_arrays([pa.array([1.0, float("nan"), None])], ["v"])
            result = create_masking_mapper(["v:date_shift:0"])(item)

            self.assertEqual(result.column("v").null_count, 2)
            self.assertTrue(pa.types.is_timestamp(result.schema.field("v").type))

    def test_a_nat_beside_a_date_reads_as_null(self):
        import pyarrow as pa

        # The smallest nanosecond timestamp reaches the mask as pandas' `NaT`, which
        # loads as NULL rather than counting as a datetime beside the date.
        both = pa.UnionArray.from_dense(
            pa.array([0, 1], pa.int8()),
            pa.array([0, 0], pa.int32()),
            [
                pa.array([date(2024, 1, 2)]),
                pa.array([-(2**63)], pa.timestamp("ns")),
            ],
        )
        for kind in (pa.Table, pa.RecordBatch):
            item = kind.from_arrays([both], ["v"])
            result = create_masking_mapper(["v:date_shift:0"])(item)

            self.assertEqual(result.column("v").to_pylist(), [date(2024, 1, 2), None])

    def test_mixed_mask_results_are_not_forced_into_a_list_column(self):
        import pyarrow as pa

        # `partial` leaves an empty list as it is and turns a list into its text.
        array = pa.array([[], ["alice"], None], pa.list_(pa.string()))
        for kind in (pa.Table, pa.RecordBatch):
            item = kind.from_arrays([array], ["v"])
            with self.assertRaises(pa.ArrowException):
                create_masking_mapper(["v:partial"])(item)

    def test_a_nan_a_mask_returns_reads_as_null(self):
        import pyarrow as pa

        for kind in (pa.Table, pa.RecordBatch):
            item = kind.from_arrays([pa.array([1.0, float("nan")])], ["v"])
            result = create_masking_mapper(["v:range:100"])(item)

            self.assertEqual(result.column("v").to_pylist(), ["0-100", None])

    def test_a_masked_field_keeps_its_metadata(self):
        import pyarrow as pa

        schema = pa.schema([pa.field("e", pa.string(), metadata={"k": "v"})])
        for kind in (pa.Table, pa.RecordBatch):
            item = kind.from_arrays([pa.array(["x"])], schema=schema)
            result = create_masking_mapper(["e:sha256"])(item)

            self.assertEqual(result.schema.field("e").metadata, {b"k": b"v"})

    def test_a_duplicate_column_name_is_refused(self):
        import pyarrow as pa

        arrays = [pa.array(["x"]), pa.array(["y"])]
        for item in (
            pa.Table.from_arrays(arrays, ["e", "e"]),
            pa.RecordBatch.from_arrays(arrays, ["e", "e"]),
        ):
            with self.assertRaisesRegex(ValueError, "more than one column"):
                create_masking_mapper(["e:redact"])(item)

    def test_an_empty_item_keeps_its_kind_and_unmasked_fields(self):
        import pyarrow as pa

        data = {
            "email": pa.array([], type=pa.string()),
            "id": pa.array([], type=pa.int64()),
        }
        for item in (pa.table(data), pa.record_batch(data)):
            result = create_masking_mapper(["email:sha256"])(item)

            self.assertIs(type(result), type(item))
            self.assertEqual(result.num_rows, 0)
            self.assertEqual(result.schema.field("id"), item.schema.field("id"))


@pytest.mark.parametrize(
    "fragment",
    ["#use_pyarrow=true", ""],
    ids=["record-batches", "rows"],
)
def test_a_masked_parquet_column_loads_hashed(tmp_path, fragment):
    # `use_pyarrow` makes the Parquet reader yield Arrow record batches; without it
    # the reader yields dict rows. Both have to load the same rows: the hash, not
    # the address, NULL as NULL, and an integer hashed as the integer it is.
    source = tmp_path / "source.parquet"
    pa.parquet.write_table(
        pa.table(
            {
                "email": ["a@example.com", "b@example.com", None],
                "id": pa.array([1, None, 2**53 + 1], type=pa.int64()),
                "big": pa.array([2**53 + 1, None, 3], type=pa.int64()),
            }
        ),
        source,
    )
    dest = tmp_path / "warehouse.duckdb"

    result = invoke_ingest_command(
        f"file://{source}{fragment}",
        "rows",
        f"duckdb:///{dest}",
        "main.t",
        mask=["email:sha256", "id:sha256"],
        print_output=False,
    )

    assert result.exit_code == 0, result.output
    with duckdb.connect(str(dest), read_only=True) as connection:
        rows = connection.execute(
            "SELECT email, id, big, typeof(big) FROM main.t ORDER BY big NULLS LAST"
        ).fetchall()

    def sha(value):
        return hashlib.sha256(str(value).encode()).hexdigest()

    assert rows == [
        (None, sha(2**53 + 1), 3, "BIGINT"),
        (sha("a@example.com"), sha(1), 2**53 + 1, "BIGINT"),
        (sha("b@example.com"), None, None, "BIGINT"),
    ]


@pytest.mark.parametrize(
    "null_first", [True, False], ids=["null-first", "values-first"]
)
def test_a_masked_column_null_in_a_whole_batch_loads(tmp_path, null_first):
    # Each row group reaches the mask as its own record batch; one holds no value
    # in the masked column, the other holds integers it hashes to text.
    nulls, values = [None] * 10_000, list(range(10_000))
    ids = nulls + values if null_first else values + nulls
    source = tmp_path / "source.parquet"
    pa.parquet.write_table(
        pa.table(
            {
                "id": pa.array(ids, pa.int64()),
                "n": pa.array(range(20_000), pa.int64()),
            }
        ),
        source,
        row_group_size=10_000,
    )
    dest = tmp_path / "warehouse.duckdb"

    result = invoke_ingest_command(
        f"file://{source}#use_pyarrow=true",
        "rows",
        f"duckdb:///{dest}",
        "main.t",
        mask=["id:sha256"],
        print_output=False,
    )

    assert result.exit_code == 0, result.output
    with duckdb.connect(str(dest), read_only=True) as connection:
        counts = connection.execute(
            "SELECT count(*), count(id), any_value(typeof(id)) FROM main.t"
        ).fetchone()
    assert counts == (20_000, 10_000, "VARCHAR")


@pytest.mark.parametrize("algorithm", ["round:10", "noise:0"])
@pytest.mark.parametrize(
    "zeros_first", [True, False], ids=["zeros-first", "values-first"]
)
def test_a_masked_int32_column_loads_whether_a_batch_changes_or_not(
    tmp_path, algorithm, zeros_first
):
    # Each row group reaches the mask as its own record batch. The mask hands the
    # zeros back as the same objects, as CPython caches small integers, and turns
    # the other row group's values into new ones; both have to load as one type.
    zeros, values = [0] * 10_000, list(range(1_000, 11_000))
    numbers = zeros + values if zeros_first else values + zeros
    source = tmp_path / "source.parquet"
    pa.parquet.write_table(
        pa.table({"n": pa.array(numbers, pa.int32())}),
        source,
        row_group_size=10_000,
    )
    dest = tmp_path / "warehouse.duckdb"

    result = invoke_ingest_command(
        f"file://{source}#use_pyarrow=true",
        "rows",
        f"duckdb:///{dest}",
        "main.t",
        mask=[f"n:{algorithm}"],
        print_output=False,
    )

    assert result.exit_code == 0, result.output
    with duckdb.connect(str(dest), read_only=True) as connection:
        counts = connection.execute(
            "SELECT count(*), count(n), sum((n = 0)::INT), any_value(typeof(n)) "
            "FROM main.t"
        ).fetchone()
    assert counts == (20_000, 20_000, 10_000, "BIGINT")


@pytest.mark.parametrize("algorithm", ["round:10", "range", "noise:0", "year_only"])
@pytest.mark.parametrize("nans", ["first", "last", "only"])
def test_a_masked_float_column_nan_in_a_whole_batch_loads(tmp_path, algorithm, nans):
    # Each row group reaches the mask as its own record batch, one with nothing
    # but NaN in the masked column. Typed `null` before the mask has typed the
    # column, it loads beside the integers `round` gives the others, or the text
    # `range` gives them, and the unmasked column keeps its rows.
    values = [float(i) + 0.5 for i in range(10_000)]
    nan = [math.nan] * 10_000
    numbers = {"first": nan + values, "last": values + nan, "only": nan + nan}[nans]
    source = tmp_path / "source.parquet"
    pa.parquet.write_table(
        pa.table({"id": range(20_000), "n": numbers}),
        source,
        row_group_size=10_000,
    )
    dest = tmp_path / "warehouse.duckdb"

    result = invoke_ingest_command(
        f"file://{source}#use_pyarrow=true",
        "rows",
        f"duckdb:///{dest}",
        "main.t",
        mask=[f"n:{algorithm}"],
        print_output=False,
    )

    assert result.exit_code == 0, result.output
    with duckdb.connect(str(dest), read_only=True) as connection:
        rows = connection.execute("SELECT count(DISTINCT id) FROM main.t").fetchone()
        masked = connection.execute(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_name = 't' AND column_name = 'n'"
        ).fetchone()
        if masked == (1,):
            masked = connection.execute("SELECT count(n) FROM main.t").fetchone()
    assert rows == (20_000,)
    assert masked == ((0,) if nans == "only" else (10_000,))


@pytest.mark.parametrize(
    ("algorithm", "source_type", "empty", "loaded"),
    [
        ("round:10", pa.int64(), None, 10_000),
        ("noise:0", pa.float64(), math.nan, 10_000),
        ("sha256", pa.int64(), None, None),
    ],
    ids=["null-then-ints", "nan-then-floats", "all-null"],
)
def test_a_masked_column_alone_keeps_the_rows_of_a_leading_empty_batch(
    tmp_path, algorithm, source_type, empty, loaded
):
    # Each row group reaches the mask as its own record batch, and the masked
    # column is all the first one holds. Typed `null`, dlt would drop it and the
    # batch's rows with it.
    values = list(range(10_000)) if loaded else [None] * 10_000
    numbers = [empty] * 10_000 + values
    source = tmp_path / "source.parquet"
    pa.parquet.write_table(
        pa.table({"n": pa.array(numbers, source_type, from_pandas=True)}),
        source,
        row_group_size=10_000,
    )
    dest = tmp_path / "warehouse.duckdb"

    result = invoke_ingest_command(
        f"file://{source}#use_pyarrow=true",
        "rows",
        f"duckdb:///{dest}",
        "main.t",
        mask=[f"n:{algorithm}"],
        print_output=False,
    )

    assert result.exit_code == 0, result.output
    with duckdb.connect(str(dest), read_only=True) as connection:
        counts = connection.execute("SELECT count(*), count(n) FROM main.t").fetchone()
    assert counts == (20_000, loaded or 0)


def test_a_masked_column_alone_typed_otherwise_after_an_empty_batch_fails(tmp_path):
    # The leading batch takes the integer type its source loads as, and the
    # hashes after it are text: dlt refuses the run rather than lose the rows.
    numbers = [None] * 10_000 + list(range(10_000))
    source = tmp_path / "source.parquet"
    pa.parquet.write_table(
        pa.table({"n": pa.array(numbers, pa.int64())}),
        source,
        row_group_size=10_000,
    )
    dest = tmp_path / "warehouse.duckdb"

    result = invoke_ingest_command(
        f"file://{source}#use_pyarrow=true",
        "rows",
        f"duckdb:///{dest}",
        "main.t",
        mask=["n:sha256"],
        print_output=False,
    )

    assert result.exit_code != 0
    assert "IncompatibleArrowSchema" in repr(result.exception) + result.output


def test_each_resource_gets_its_own_mapper_and_shares_the_tokens(tmp_path, monkeypatch):
    # A mapper remembers the types it gave its resource's columns, so each
    # resource needs its own; one engine keeps a value's token across them.
    import dlt

    import omniload.codec.filter as codec_filter
    from omniload.api import run_ingest
    from omniload.core.factory import SourceDestinationFactory

    class TwoResources:
        def handles_incrementality(self):
            return True

        def dlt_source(self, uri, table, **kwargs):
            @dlt.resource(name="a")
            def a():
                yield pa.table({"id": ["x", "y"]})

            @dlt.resource(name="b")
            def b():
                yield pa.table({"id": ["y", "z"]})

            @dlt.source
            def both():
                return a(), b()

            return both()

    engines = []
    build = codec_filter.create_masking_filter

    def spy(mask_configs, engine=None):
        engines.append(engine)
        return build(mask_configs, engine)

    monkeypatch.setattr(codec_filter, "create_masking_filter", spy)
    monkeypatch.setattr(
        SourceDestinationFactory, "get_source", lambda self: TwoResources()
    )
    dest = tmp_path / "warehouse.duckdb"

    run_ingest(
        source_uri="influxdb://placeholder",
        dest_uri=f"duckdb:///{dest}",
        source_table="rows",
        dest_table="main.t",
        progress="log",
        mask=["id:sequential"],
    )

    assert len(engines) == 2
    assert engines[0] is engines[1] is not None
    with duckdb.connect(str(dest), read_only=True) as connection:
        ids = connection.execute("SELECT id FROM main.t ORDER BY id").fetchall()
    assert ids == [(1,), (2,), (2,), (3,)]


if __name__ == "__main__":
    unittest.main()
