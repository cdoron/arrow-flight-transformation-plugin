from fybrik_python_transformation import Action
import pyarrow as pa
from pyarrow import csv

class Redact2(Action):
    def __init__(self, description, columns, options):
        super().__init__(description, columns, options)
        self.redact_value = options.get("redactValue", "XXXXXXXXXX")

    def __call__(self, records: pa.RecordBatch) -> pa.RecordBatch:
        """Transformation logic for Redact action.

        Args:
            records (pa.RecordBatch): record batch to transform

        Returns:
            pa.RecordBatch: transformed record batch
        """
        columns = [column for column in self.columns if column in records.schema.names]
        indices = [records.schema.get_field_index(c) for c in columns]
        constColumn = pa.array([self.redact_value] * len(records), type=pa.string())
        new_columns = records.columns
        for i in indices:
            new_columns[i] = constColumn
        new_schema = self.schema(records.schema)
        return pa.RecordBatch.from_arrays(new_columns, schema=new_schema)

    def field_type(self):
        """Overrides field_type to calculate transformed schema correctly."""
        return pa.string() # redacted value is a string

a = Redact2(description="", columns=["amount"], options={"redactValue": "YYY"})
dataset="PS_20174392719_1491204439457_log.csv"
table = csv.read_csv(dataset)
batches = table.to_batches()
for b in batches:
    print(b.to_pandas())
    print(a.__call__(b).to_pandas())
