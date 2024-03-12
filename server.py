from typing import Callable, Dict, Tuple, Iterator
from os import environ

from pandas import DataFrame

import pyarrow as pa
from pyarrow.flight import (
    FlightInfo,
    FlightServerBase,
    ServerCallContext,
    FlightDescriptor,
    FlightEndpoint,
    MetadataRecordBatchReader,
    MetadataRecordBatchWriter,
)
from sklearn.linear_model import LinearRegression


class SkLearnServer(FlightServerBase):
    def __init__(
        self,
        location: str,
        models: Dict[str, Tuple[pa.Schema, Callable[[DataFrame], DataFrame]]],
    ):
        super().__init__(location)
        self.location = location
        self.models = models

    @staticmethod
    def log_context(context: ServerCallContext):
        print("connected", context.peer())

    def list_flights(
        self,
        context: ServerCallContext,
        criteria: bytes,
    ) -> Iterator[FlightInfo]:
        self.log_context(context)

        return [
            FlightInfo(
                schema=schema,
                descriptor=FlightDescriptor.for_path(name),
                endpoints=[FlightEndpoint(name, [self.location])],
                total_records=-1,
                total_bytes=-1,
            )
            for name, (schema, _) in self.models.items()
        ]

    def do_exchange(
        self,
        context: ServerCallContext,
        descriptor: FlightDescriptor,
        reader: MetadataRecordBatchReader,
        writer: MetadataRecordBatchWriter,
    ):
        self.log_context(context)
        path = descriptor.path[0].decode()
        if path in self.models.keys():
            data = reader.read_pandas()
            _, op = self.models[path]
            result = op(data)

            table = pa.Table.from_pandas(result)
            writer.begin(table.schema)
            writer.write_table(table)
        else:
            raise Exception(f"invalid path: {path}")


if __name__ == "__main__":

    def linear_regression(data: DataFrame) -> DataFrame:
        reg = LinearRegression().fit(data[["x"]], data["y"])
        data["predict"] = reg.predict(data[["x"]])
        return data

    host = environ.get("HOST", "0.0.0.0")
    port = environ.get("PORT", "8080")

    server = SkLearnServer(
        location=f"grpc://{host}:{port}",
        models={
            "linear_regression": (
                pa.schema(
                    [
                        pa.field("x", pa.float64()),
                        pa.field("y", pa.float64()),
                    ]
                ),
                linear_regression,
            )
        },
    )
    print(f"running on {server.location}. press ctl+c to exit")
    server.serve()
